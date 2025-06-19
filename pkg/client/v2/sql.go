package v2

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type SQLClient struct {
	*sql.DB // embedd sql.DB

	username string
	password string
	host     string
	scheme   string
	database string
}

func NewSQLClientWithHealthCheck(dsn string) (*SQLClient, error) {
	client, err := NewSQLClient(dsn)
	if err != nil {
		return nil, err
	}

	err = client.HealthCheck()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func NewSQLClient(dsn string) (*SQLClient, error) {
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
	if scheme == "tcp" {
		host = strings.ReplaceAll(host, ":9000", ":8123")
		scheme = "http"
	}

	// Set session timeout to 48h (172800s)
	q := parsedURL.Query()
	q.Set("max_execution_time", "10")
	parsedURL.RawQuery = q.Encode()
	parsedURL.Scheme = scheme
	parsedURL.Host = host
	dsn = parsedURL.String()

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}

	return &SQLClient{
		DB:       db, // embed sql
		username: username,
		password: password,
		host:     host,
		scheme:   scheme,
		database: database,
	}, nil
}

func (c *SQLClient) Database() string {
	return c.database
}

// Get the database name from the dsn string. If not found then return "default"
func getDatabaseNameFromDSN(dsn string) string {
	// Check if the DSN is in URL format
	if strings.Contains(dsn, "://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "iris"
		}
		return strings.TrimLeft(parsed.Path, "/")
	}

	// Otherwise, assume it's a traditional DSN format (e.g., MySQL, PostgreSQL)
	parts := strings.Split(dsn, "/")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}

	return "iris"
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
	if err := a.Ping(); err != nil {
		return err
	}
	return nil
}

func (a *SQLClient) TableEmpty(tableName string) (bool, error) {
	query := `
SELECT count() = 0 AS table_empty 
FROM system.tables 
WHERE database = '%s' AND name = '%s'
;` // end of the query

	formattedQuery := fmt.Sprintf(
		query,
		a.Database(),
		tableName,
	)

	var tableEmpty bool
	if err := a.QueryRow(formattedQuery).Scan(&tableEmpty); err != nil {
		return false, err
	} else {
		return tableEmpty, nil
	}
}
