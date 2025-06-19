package v2

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPSQLClient struct {
	username string
	password string
	host     string
	scheme   string
	database string
}

func NewHTTPSQLClient(dsn string) (*HTTPSQLClient, error) {
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	username := parsedURL.User.Username()
	password, _ := parsedURL.User.Password()
	host := parsedURL.Host
	scheme := parsedURL.Scheme

	database := getDatabaseNameFromDSN(parsedURL.String())

	q := parsedURL.Query()
	parsedURL.RawQuery = q.Encode()
	parsedURL.Scheme = scheme
	parsedURL.Host = host
	dsn = parsedURL.String()

	c := &HTTPSQLClient{
		username: username,
		password: password,
		host:     host,
		scheme:   scheme,
		database: database,
	}

	return c, nil
}

func (c *HTTPSQLClient) Download(tableName string, useGzip bool) (io.ReadCloser, error) {
	reqURL := fmt.Sprintf("%s://%s", c.scheme, c.host)
	if c.database != "" {
		reqURL += fmt.Sprintf("/?database=%s&max_execution_time=0&receive_timeout=360000&send_timeout=360000&http_receive_timeout=360000", c.database)
	}

	var body io.Reader
	var buf bytes.Buffer
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1000 FORMAT CSV", tableName)

	if useGzip {
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write([]byte(query))
		if err != nil {
			return nil, err
		}
		if err := gzipWriter.Close(); err != nil {
			return nil, err
		}
		body = &buf
	} else {
		body = strings.NewReader(query)
	}

	req, err := http.NewRequest("POST", reqURL, body)
	if err != nil {
		return nil, err
	}

	auth := c.username + ":" + c.password
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	// req.Header.Set("Content-Type", "text/plain")

	if useGzip {
		req.Header.Set("Content-Encoding", "gzip") // Tell server we send gzip
		req.Header.Set("Accept-Encoding", "gzip")  // Ask for gzip response
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// output the raw gzip output
	return resp.Body, nil
}

func (c *HTTPSQLClient) Upload(tableName string, csvData io.Reader, useGzip bool) (*http.Response, error) {
	reqURL := fmt.Sprintf(
		"%s://%s/?query=%s&max_execution_time=0&receive_timeout=360000&send_timeout=360000&http_receive_timeout=360000",
		c.scheme,
		c.host,
		url.QueryEscape(fmt.Sprintf("INSERT INTO %s FORMAT CSV", tableName)),
	)

	req, err := http.NewRequest("POST", reqURL, csvData)
	if err != nil {
		return nil, err
	}

	// Set headers
	auth := c.username + ":" + c.password
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	// req.Header.Set("Content-Type", "")
	if useGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	return http.DefaultClient.Do(req)
}

// func NewHTTPSQLClientWithHealthCheck(dsn string) (*SQLClient, error) {
// 	client, err := NewSQLClient(dsn)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	err = client.HealthCheck()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return client, nil
// }
//
// func NewHTTPSQLClient(dsn string) (*SQLClient, error) {
// 	parsedURL, err := url.Parse(dsn)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	username := parsedURL.User.Username()
// 	password, _ := parsedURL.User.Password()
// 	host := parsedURL.Host
// 	scheme := parsedURL.Scheme
//
// 	database := getDatabaseNameFromDSN(parsedURL.String())
//
// 	// This is required because we cannot use the tcp port with http
// 	if scheme == "tcp" {
// 		host = strings.ReplaceAll(host, ":9000", ":8123")
// 		scheme = "http"
// 	}
//
// 	// Set session timeout to 48h (172800s)
// 	q := parsedURL.Query()
// 	parsedURL.RawQuery = q.Encode()
// 	parsedURL.Scheme = scheme
// 	parsedURL.Host = host
// 	dsn = parsedURL.String()
//
// 	db, err := sql.Open("clickhouse", dsn)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return &SQLClient{
// 		DB:       db, // embed sql
// 		username: username,
// 		password: password,
// 		host:     host,
// 		scheme:   scheme,
// 		database: database,
// 	}, nil
// }
//
// func (c *SQLClient) Database() string {
// 	return c.database
// }
//
// // Get the database name from the dsn string. If not found then return "default"
// func getDatabaseNameFromDSN(dsn string) string {
// 	// Check if the DSN is in URL format
// 	if strings.Contains(dsn, "://") {
// 		parsed, err := url.Parse(dsn)
// 		if err != nil {
// 			return "iris"
// 		}
// 		return strings.TrimLeft(parsed.Path, "/")
// 	}
//
// 	// Otherwise, assume it's a traditional DSN format (e.g., MySQL, PostgreSQL)
// 	parts := strings.Split(dsn, "/")
// 	if len(parts) > 1 {
// 		return parts[len(parts)-1]
// 	}
//
// 	return "iris"
// }
//
// // Note that the query should not contain any newline.
// func (a *SQLClient) Download(query string) (io.ReadCloser, error) {
// 	baseURL := &url.URL{
// 		Scheme: a.scheme,
// 		Host:   a.host,
// 	}
// 	params := url.Values{}
// 	params.Set("database", a.database)
// 	params.Set("query", query)
// 	baseURL.RawQuery = params.Encode()
//
// 	req, err := http.NewRequest("POST", baseURL.String(), nil)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	req.Header.Set("Accept-Encoding", "gzip")
// 	req.Header.Set("Content-Type", "application/json")
//
// 	req.SetBasicAuth(a.username, a.password)
//
// 	httpClient := &http.Client{
// 		Timeout: 30 * 60 * time.Second,
// 	}
// 	resp, err := httpClient.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	if resp.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("upload failed with status: %s", resp.Status)
// 	}
//
// 	return resp.Body, nil
// }
//
// // Note that the query should not contain any newline.
// func (a *SQLClient) Upload(query string, r io.Reader) (io.ReadCloser, error) {
// 	baseURL := &url.URL{
// 		Scheme: a.scheme,
// 		Host:   a.host,
// 	}
//
// 	params := url.Values{}
// 	params.Set("database", a.database)
// 	params.Set("query", query)
// 	baseURL.RawQuery = params.Encode()
//
// 	req, err := http.NewRequest("POST", baseURL.String(), r)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	req.SetBasicAuth(a.username, a.password)
// 	req.Header.Set("Content-Type", "application/octet-stream")
//
// 	httpClient := &http.Client{
// 		Timeout: 30 * 60 * time.Second,
// 	}
// 	resp, err := httpClient.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	if resp.StatusCode != http.StatusOK {
// 		return nil, fmt.Errorf("upload failed with status: %s", resp.Status)
// 	}
//
// 	return resp.Body, nil
// }
//
// func (a *SQLClient) HealthCheck() error {
// 	if err := a.Ping(); err != nil {
// 		return err
// 	}
// 	return nil
// }
//
// func (a *SQLClient) TableEmpty(tableName string) (bool, error) {
// 	query := `
// SELECT count() = 0 AS table_empty
// FROM system.tables
// WHERE database = '%s' AND name = '%s'
// ;` // end of the query
//
// 	formattedQuery := fmt.Sprintf(
// 		query,
// 		a.Database(),
// 		tableName,
// 	)
//
// 	var tableEmpty bool
// 	if err := a.QueryRow(formattedQuery).Scan(&tableEmpty); err != nil {
// 		return false, err
// 	} else {
// 		return tableEmpty, nil
// 	}
// }
