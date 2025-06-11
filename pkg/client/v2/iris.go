package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"

	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type IrisClient struct {
	jwt     string
	baseURL string
}

func NewIrisClientWithJWT() (*IrisClient, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path.Join(homeDir, config.DefaultIrisctlJWTPath))
	if err != nil {
		return nil, err
	}

	return &IrisClient{
		jwt:     string(content),
		baseURL: config.DefaultIrisAPIURL,
	}, nil
}

func (c *IrisClient) GetAllMeasurements(ipv4, finished bool) ([]apiv3.IrisctlMeasurement, error) {
	tagString := config.DefaultIPv4Tag
	if !ipv4 {
		tagString = config.DefaultIPv6Tag
	}

	results, err := c.GetMeasurements(tagString, config.MaxIrisAPILimit, 0, finished)
	if err != nil {
		return nil, err
	}
	numCallsToMake := int(math.Ceil(float64(results.Count)/float64(config.MaxIrisAPILimit))) - 1
	measurements := make([]apiv3.IrisctlMeasurement, 0, results.Count)
	measurements = append(measurements, results.Results...)

	for i := 0; i < numCallsToMake; i++ {
		offset := (i + 1) * config.MaxIrisAPILimit
		results, err := c.GetMeasurements(tagString, config.MaxIrisAPILimit, offset, finished)
		if err != nil {
			return nil, err
		}

		measurements = append(measurements, results.Results...)
	}

	return measurements, nil
}

func (c *IrisClient) GetMeasurements(tag string, limit, offset int, finished bool) (*apiv3.IrisctlMeasurementResponse, error) {
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
