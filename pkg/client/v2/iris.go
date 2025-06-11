package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"

	v1 "github.com/dioptra-io/ufuk-research/api/v1"
	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type IrisClient struct {
	jwt     string
	baseURL string
}

func NewIrisClientWithJWT() (*IrisClient, error) {
	err := refreshToken()
	if err != nil {
		return nil, err
	}

	token, err := readToken()
	if err != nil {
		return nil, err
	}

	c := &IrisClient{
		jwt:     token,
		baseURL: config.DefaultIrisAPIURL,
	}

	return c, nil
}

func readToken() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	content, err := os.ReadFile(path.Join(homeDir, config.DefaultIrisctlJWTPath))
	if err != nil {
		return "", err
	}

	return string(content), err
}

func (c *IrisClient) GetTableNamesFor(ipv4, finished bool, date v1.Date) ([]string, error) {
	var tableNames []string
	meas, err := c.GetAllMeasurementsOn(ipv4, finished, date)
	if err != nil {
		return nil, err
	}

	for _, m := range meas {
		for _, a := range m.Agents {
			tableName := fmt.Sprintf("results__%s__%s", strings.ReplaceAll(m.UUID, "-", "_"), strings.ReplaceAll(a.AgentUUID, "-", "_"))
			tableNames = append(tableNames, tableName)
		}
	}

	return tableNames, nil
}

func (c *IrisClient) GetAllMeasurementsOn(ipv4, finished bool, date v1.Date) ([]apiv3.IrisctlMeasurement, error) {
	meas, err := c.GetAllMeasurements(ipv4, finished)
	if err != nil {
		return nil, err
	}

	var filtered []apiv3.IrisctlMeasurement
	for _, m := range meas {
		if date.Contains(m.StartTime.Time) {
			filtered = append(filtered, m)
		}
	}

	return filtered, nil
}

func refreshToken() error {
	cmd := exec.Command("irisctl", "meas")

	_, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	return nil
}

func (c *IrisClient) GetAllMeasurements(ipv4, finished bool) ([]apiv3.IrisctlMeasurement, error) {
	tagString := config.DefaultIPv4Tag
	if !ipv4 {
		tagString = config.DefaultIPv6Tag
	}

	results, err := c.getMeasurements(tagString, config.MaxIrisAPILimit, 0, finished)
	if err != nil {
		return nil, err
	}
	numCallsToMake := int(math.Ceil(float64(results.Count)/float64(config.MaxIrisAPILimit))) - 1
	measurements := make([]apiv3.IrisctlMeasurement, 0, results.Count)
	measurements = append(measurements, results.Results...)

	for i := 0; i < numCallsToMake; i++ {
		offset := (i + 1) * config.MaxIrisAPILimit
		results, err := c.getMeasurements(tagString, config.MaxIrisAPILimit, offset, finished)
		if err != nil {
			return nil, err
		}

		measurements = append(measurements, results.Results...)
	}

	return measurements, nil
}

func (c *IrisClient) getMeasurements(tag string, limit, offset int, finished bool) (*apiv3.IrisctlMeasurementResponse, error) {
	finishedString := ""
	if finished {
		finishedString = "state=finished&"
	}

	url := fmt.Sprintf("%s/measurements/?only_mine=false&%stag=%s&offset=%d&limit=%d", c.baseURL, finishedString, tag, offset, limit)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("User-Agent", "irisctl")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.jwt))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("given status code is not 200")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var responseObj apiv3.IrisctlMeasurementResponse
	if err := json.Unmarshal(body, &responseObj); err != nil {
		return nil, err
	}

	return &responseObj, nil
}
