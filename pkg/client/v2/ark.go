package v2

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
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

type ArkClient struct {
	username string
	password string
}

func NewArkClient(username, password string) *ArkClient {
	return &ArkClient{
		username: username,
		password: password,
	}
}

func (c *ArkClient) GetArkCycles(ctx context.Context, dates []apiv1.Date) ([]apiv1.ArkCycle, error) {
	// util.ExtractDigitHrefLinks(r io.Reader)
	arkCycles := make([]apiv1.ArkCycle, 0)
	for _, date := range dates {
		arkCyclesTemp := apiv1.ArkCycle{
			Date: date,
		}
		arkCycles = append(arkCycles, arkCyclesTemp)
	}
	return arkCycles, nil
}

func (c *ArkClient) GetWartFiles(ctx context.Context, cycles []apiv1.ArkCycle) ([]apiv1.WartFile, error) {
	arkWartFiles := make([]apiv1.WartFile, 0)

	for _, cycle := range cycles {
		req, err := http.NewRequest("GET", cycle.GetURL(), nil)
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
			wartFile := apiv1.WartFile{
				URL: fmt.Sprintf("%s/%s", cycle.GetURL(), wartFilename),
			}
			arkWartFiles = append(arkWartFiles, wartFile)
		}
	}

	return arkWartFiles, nil
}

// this downloads the wart files and unzips it using gzip
func (c *ArkClient) DownloadRouteTraces(ctx context.Context, wart apiv1.WartFile) (<-chan apiv1.ResultsTableRow, error) {
	wartReader, err := newWartReader(ctx, wart, c.username, c.password)
	if err != nil {
		return nil, err
	}
	defer wartReader.Close()

	unzipReader, err := gzip.NewReader(wartReader)
	if err != nil {
		return nil, err
	}
	defer unzipReader.Close()

	pantraceReader, err := newPantraceReader(unzipReader)
	if err != nil {
		return nil, err
	}
	defer pantraceReader.Close()

	resultsTableRowReader := newResultsTableRowReader(pantraceReader)

	return resultsTableRowReader, nil
}

// Creates a new wart reader which reads from the main database of ark. It performs a http request.
func newWartReader(ctx context.Context, wartFile apiv1.WartFile, username, password string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", wartFile.URL, nil)
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

// Note that the actual copying is done on a separate go routine. This means that if there
// an error on the conversion we won't be able to see the actual error. Instead we would
// only see the log, and the actual error will only be realized when trying to read from
// this function's output.
//
// To overcome this we might return an error chan instead of just an error. But I am too
// lazy to implement that rn.
func newPantraceReader(r io.Reader) (io.ReadCloser, error) {
	logger := util.GetLogger()

	pantrace := exec.Command(
		"pantrace",
		"--from",
		"scamper-trace-warts",
		"--to",
		"iris")

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

	// This is for piping the given reader as an argument to the stdin of the process.
	// A better method might be the usage of pipes, but for now this is works.
	go func() {
		defer stdin.Close()
		if numBytesRead, err := io.Copy(stdin, r); err != nil {
			logger.Panicf("Error while converting the wart using pantrace: %v.\n", err)
		} else {
			logger.Debugf("Conversion with pantrace resulted %v bytes.\n", numBytesRead)
		}
	}()

	// This is for displaying the error message pantrace gives out
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

func newResultsTableRowReader(r io.Reader) <-chan apiv1.ResultsTableRow {
	// Reply holds the information from the target’s response
	type Reply struct {
		Timestamp time.Time `json:"timestamp"`
		QuotedTTL int       `json:"quoted_ttl"`
		TTL       int       `json:"ttl"`
		Size      int       `json:"size"`
		Addr      string    `json:"addr"`
		ICMPType  int       `json:"icmp_type"`
		ICMPCode  int       `json:"icmp_code"`
		// MPLSLabels [][4]int  `json:"mpls_labels"`
		RTT float64 `json:"rtt"`
	}

	// Probe is one ping/probe attempt
	type Probe struct {
		Timestamp time.Time `json:"timestamp"`
		Size      int       `json:"size"`
		Reply     *Reply    `json:"reply"`
	}

	// Hop groups a set of probes at a given TTL
	type Hop struct {
		TTL    int     `json:"ttl"`
		Probes []Probe `json:"probes"`
	}

	// Flow is one of potentially many in a Measurement
	type Flow struct {
		SrcPort int   `json:"src_port"`
		DstPort int   `json:"dst_port"`
		Hops    []Hop `json:"hops"`
	}

	// Measurement corresponds to the top‐level JSON object
	type Measurement struct {
		MeasurementName string    `json:"measurement_name"`
		MeasurementID   string    `json:"measurement_id"`
		AgentID         string    `json:"agent_id"`
		StartTime       time.Time `json:"start_time"`
		EndTime         time.Time `json:"end_time"`
		Protocol        string    `json:"protocol"`
		SrcAddr         string    `json:"src_addr"`
		SrcAddrPublic   *string   `json:"src_addr_public"`
		DstAddr         string    `json:"dst_addr"`
		Flows           []Flow    `json:"flows"`
	}

	ch := make(chan apiv1.ResultsTableRow)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			var m Measurement
			if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
				log.Printf("failed to unmarshal measurement: %v", err)
				continue
			}

			for _, flow := range m.Flows {
				for _, hop := range flow.Hops {
					for _, probe := range hop.Probes {
						// Skip if no reply
						if probe.Reply == nil {
							continue
						}

						// Convert MPLS labels
						var labels [][4]uint32
						// MPLS Labes are not read for now.
						// for _, label := range probe.Reply.MPLSLabels {
						// 	// Assuming a placeholder since your JSON shows []int, but ClickHouse uses Tuple
						// 	// This step will depend on actual label structure
						// 	labels = append(labels, [4]uint32{label, 0, 0, 0})
						// }

						row := apiv1.ResultsTableRow{
							CaptureTimestamp: probe.Reply.Timestamp.Truncate(time.Second), // 1-second resolution
							ProbeProtocol:    util.ProtocolToUint8(m.Protocol),
							ProbeSrcAddr:     m.SrcAddr,
							ProbeDstAddr:     m.DstAddr,
							ProbeSrcPort:     uint16(flow.SrcPort),
							ProbeDstPort:     uint16(flow.DstPort),
							ProbeTTL:         uint8(hop.TTL),
							QuotedTTL:        uint8(probe.Reply.QuotedTTL),
							ReplySrcAddr:     probe.Reply.Addr,
							ReplyProtocol:    util.ProtocolToUint8(m.Protocol),
							ReplyICMPType:    uint8(probe.Reply.ICMPType),
							ReplyICMPCode:    uint8(probe.Reply.ICMPCode),
							ReplyTTL:         uint8(probe.Reply.TTL),
							ReplySize:        uint16(probe.Reply.Size),
							ReplyMPLSLabels:  labels,
							RTT:              uint16(probe.Reply.RTT), // Potential truncation — handle overflow if RTT > 65535
							Round:            1,                       // Placeholder — you can inject it externally
						}
						ch <- row
					}
				}
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("error scanning measurements: %v", err)
		}
	}()
	return ch
}
