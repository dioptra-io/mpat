package v3

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	v3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

type ArkClient struct {
	username string
	password string
}

func NewArkClient(username, password string) (*ArkClient, error) {
	return &ArkClient{
		username: username,
		password: password,
	}, nil
}

func (c *ArkClient) GetArkCycles(ctx context.Context, dates []apiv1.Date) ([]apiv1.ArkCycle, error) {
	arkCycles := make([]apiv1.ArkCycle, 0)
	for _, date := range dates {
		arkCyclesTemp := apiv1.ArkCycle{
			Date: date,
		}
		arkCycles = append(arkCycles, arkCyclesTemp)
	}
	return arkCycles, nil
}

func (c *ArkClient) GetWartFiles(ctx context.Context, t time.Time) ([]string, error) {
	arkWartFiles := make([]string, 0)

	req, err := http.NewRequest("GET", getCycleURL(t), nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.username, c.password)

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	content := string(body)

	re := regexp.MustCompile("\".*gz\"")

	matches := re.FindAllString(content, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no match for the cycle-page")
	}

	for i := 0; i < len(matches); i++ {
		wartFilename := strings.ReplaceAll(matches[i], "\"", "")
		wartFileURL := fmt.Sprintf("%s/%s", getCycleURL(t), wartFilename)
		arkWartFiles = append(arkWartFiles, wartFileURL)
	}

	return arkWartFiles, nil
}

func getCycleString(t time.Time) string {
	return fmt.Sprintf("cycle-%d%02d%02d", t.Year(), t.Month(), t.Day())
}

func getCycleURL(t time.Time) string {
	return fmt.Sprintf("%s/%d/%s", config.DefaultArkIPv4DatabaseBaseURL, t.Year(), getCycleString(t))
}

func (c *ArkClient) DownloadRouteTraces(ctx context.Context, wartURL string) (<-chan *v3.IrisResultsRow, error) {
	wartReader, err := newWartReader(ctx, wartURL, c.username, c.password)
	if err != nil {
		return nil, err
	}

	unzipReader, err := newGzipReader(wartReader)
	if err != nil {
		return nil, err
	}

	pantraceReader, err := newPantraceReader(unzipReader)
	if err != nil {
		return nil, err
	}

	resultsTableRowReader := newResultsTableRowReader(pantraceReader)

	return resultsTableRowReader, nil
}

func newGzipReader(r io.ReadCloser) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		defer r.Close()
		defer pw.Close()

		gzipReader, err := gzip.NewReader(r)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("gzip reader error: %w", err))
			return
		}
		defer gzipReader.Close()

		if _, err := io.Copy(pw, gzipReader); err != nil {
			pw.CloseWithError(fmt.Errorf("copy error: %w", err))
		}
	}()

	return pr, nil
}

func newWartReader(ctx context.Context, wartURL string, username, password string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", wartURL, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(username, password)

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func newPantraceReader(r io.ReadCloser) (io.ReadCloser, error) {
	logger := util.GetLogger()

	pantrace := exec.Command(
		"pantrace",
		"--from",
		"scamper-trace-warts",
		"--to",
		"flat")

	stdin, err := pantrace.StdinPipe()
	if err != nil {
		return nil, err
	}

	stderr, err := pantrace.StderrPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := pantrace.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := pantrace.Start(); err != nil {
		return nil, err
	}

	go func() {
		defer stdin.Close()
		defer r.Close()
		if numBytesRead, err := io.Copy(stdin, r); err != nil {
			logger.Panicf("Error while converting the wart using pantrace: %v.\n", err)
		} else {
			logger.Debugf("Conversion with pantrace resulted %v bytes.\n", numBytesRead)
		}
	}()

	go func() {
		defer stderr.Close()
		if data, err := io.ReadAll(stderr); err != nil {
			logger.Panicf("Error while reading the std err of pantrace comamnd: %v.\n", err)
		} else if len(data) != 0 {
			logger.Panicf("Error while converting the pantrace: %v.\n", string(data))
		}
	}()

	return stdout, nil
}

func newResultsTableRowReader(r io.ReadCloser) <-chan *v3.IrisResultsRow {
	ch := make(chan *v3.IrisResultsRow)
	go func() {
		defer close(ch)
		defer r.Close()

		scanner := bufio.NewScanner(r)

		buf := make([]byte, 0, 1024*1024) // 1 MB buffer for the scanner
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			var measurementList []v3.IrisResultsRow

			if err := json.Unmarshal(scanner.Bytes(), &measurementList); err != nil {
				log.Printf("failed to unmarshal measurement: %v", err)
				continue
			}

			for _, measurement := range measurementList {
				m := measurement // shallow copy
				ch <- &m
			}

		}
		if err := scanner.Err(); err != nil {
			log.Printf("error scanning measurements: %v", err)
		}
	}()
	return ch
}
