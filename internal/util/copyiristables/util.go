package copyiristables

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/chigopher/pathlib"
)

// This is the config file that the database uses.
type DatabaseConfig struct {
	User      string
	Password  string
	Database  string
	Host      string
	ChunkSize int
	TableType string
}

func (c *DatabaseConfig) GetTablesOfMeasUUID(measUUID string) ([]string, error) {
	time.Sleep(time.Second)
	return []string{}, nil
}

func (c *DatabaseConfig) CreateResultsTable() error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) DownloadResultsTable(file *pathlib.Path) error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) UploadResultsTable(file *pathlib.Path) error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) GetSizeOfResultsTable(tableName string) (int, error) {
	time.Sleep(time.Second)
	return 2, nil
}

func (c *DatabaseConfig) TableExists(tableName string) (bool, error) {
	time.Sleep(time.Second)
	return false, nil
}

func (c *DatabaseConfig) Execute(query string, data []byte) (*http.Response, error) {
	ctx := context.Background()

	req, err := http.NewRequestWithContext(ctx, "POST", c.Host, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")

	if c.User != "" && c.Password != "" {
		req.SetBasicAuth(c.User, c.Password)
	}

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if err := raiseForStatus(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *DatabaseConfig) Text(query string) (string, error) {
	resp, err := c.Execute(query, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func raiseForStatus(resp *http.Response) error {
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return errors.New(string(body))
	}
	return nil
}
