package v1

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v1 "github.com/dioptra-io/ufuk-research/api/v1"
)

type SQLClient struct {
	*sql.DB // embedd sql.DB

	username string
	password string
	host     string
	scheme   string
	database string
}

func NewSQLClient(dsn string) (*SQLClient, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	username := parsedURL.User.Username()
	password, _ := parsedURL.User.Password()
	host := parsedURL.Host
	scheme := parsedURL.Scheme

	database := getDatabaseNameFromDSN(parsedURL.String())

	// This is required because we cannot use the tcp port with http
	// host = strings.ReplaceAll(host, ":9000", ":8123")

	return &SQLClient{
		DB:       db, // embed sql
		username: username,
		password: password,
		host:     host,
		scheme:   scheme,
		database: database,
	}, nil
}

// Get the database name from the dsn string. If not found then return "default"
func getDatabaseNameFromDSN(dsn string) string {
	// Check if the DSN is in URL format
	if strings.Contains(dsn, "://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "default"
		}
		return strings.TrimLeft(parsed.Path, "/")
	}

	// Otherwise, assume it's a traditional DSN format (e.g., MySQL, PostgreSQL)
	parts := strings.Split(dsn, "/")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}

	return "default"
}

// Note that the query should not contain any newline.
func (a *SQLClient) Download(query string) (io.ReadCloser, error) {
	baseURL := &url.URL{
		Scheme: a.scheme,
		Host:   a.host,
	}
	params := url.Values{}
	params.Set("database", a.database)
	params.Set("query", query)
	baseURL.RawQuery = params.Encode()

	req, err := http.NewRequest("POST", baseURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// req.Header.Set("Accept-Encoding", "gzip")
	// req.Header.Set("Content-Type", "application/json")

	req.SetBasicAuth(a.username, a.password)

	httpClient := &http.Client{
		Timeout: 30 * 60 * time.Second,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upload failed with status: %s", resp.Status)
	}

	return resp.Body, nil
}

// Note that the query should not contain any newline.
func (a *SQLClient) Upload(query string, r io.Reader) (io.ReadCloser, error) {
	baseURL := &url.URL{
		Scheme: a.scheme,
		Host:   a.host,
	}

	params := url.Values{}
	params.Set("database", a.database)
	params.Set("query", query)
	baseURL.RawQuery = params.Encode()

	req, err := http.NewRequest("POST", baseURL.String(), r)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(a.username, a.password)
	req.Header.Set("Content-Type", "application/octet-stream")

	httpClient := &http.Client{
		Timeout: 30 * 60 * time.Second,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upload failed with status: %s", resp.Status)
	}

	return resp.Body, nil
}

func (a *SQLClient) HealthCheck() error {
	panic("SQLClient HealthCheck()")
}

func (a *SQLClient) GetTableInfo(tablesToCheck []string) ([]v1.ResultsTableInfo, error) {
	panic("SQLClient GetTableInfo")
}

func (a *SQLClient) DropTableIfNotExists(tableName string) error {
	panic("SQLClient GetTableInfo")
}

func (a *SQLClient) CreateResultsTableIfNotExists(tableName string) error {
	panic("SQLClient GetTableInfo")
}
