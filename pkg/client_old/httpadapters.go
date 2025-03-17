package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type basicClickHouseHTTPAdapter struct {
	username string
	password string
	host     string
	scheme   string
	database string
}

func newBasicClickHouseHTTPAdapter(dsn *url.URL) ClickHouseHTTPAdapter {
	username := dsn.User.Username()
	password, ok := dsn.User.Password()
	if !ok {
		password = ""
	}
	host := dsn.Host
	urlScheme := "http"
	if dsn.Scheme == "https" {
		urlScheme = "https"
	}
	database := getDatabaseNameFromDSN(dsn.String())

	// This is required because we cannot use the tcp port with http
	host = strings.ReplaceAll(host, ":9000", ":8123")

	return &basicClickHouseHTTPAdapter{
		username: username,
		password: password,
		host:     host,
		scheme:   urlScheme,
		database: database,
	}
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

func (a *basicClickHouseHTTPAdapter) Close() error {
	return nil
}

// Note that the query should not contain any newline.
func (a *basicClickHouseHTTPAdapter) Download(query string) (io.ReadCloser, error) {
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
func (a *basicClickHouseHTTPAdapter) Upload(query string, r io.Reader) (io.ReadCloser, error) {
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

type basicArkHTTPAdapter struct {
	username string
	password string
	baseUrl  string
}

// The base URL for cycles. It is hardcoded :). This should not end with '/'
const ARK_DATASET_BASE_URL = "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily"

// The base URL is defined as a const since it is not likely to change.
func newBasicArkHTTPAdapter(username, password string) ArkHTTPAdapter {
	return &basicArkHTTPAdapter{
		username: username,
		password: password,
		baseUrl:  ARK_DATASET_BASE_URL,
	}
}

// Does nothing for now.
func (a *basicArkHTTPAdapter) Close() error {
	return nil
}

func (a *basicArkHTTPAdapter) timeToDateString(t time.Time) string {
	return fmt.Sprintf("%d%02d%02d", t.Year(), int(t.Month()), int(t.Day()))
}

func (a *basicArkHTTPAdapter) timeToURL(t time.Time) string {
	dateString := a.timeToDateString(t)
	generatedURL := fmt.Sprintf("%s/%v/cycle-%s", a.baseUrl, t.Year(), dateString)
	return generatedURL
}

func (a *basicArkHTTPAdapter) WartLinks(t time.Time) ([]string, error) {
	// Get the current URL of the cycle page.
	currentURL := a.timeToURL(t)

	req, err := http.NewRequest("GET", currentURL, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(a.username, a.password)

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

	urlsToDownload := make([]string, len(matches))

	for i := 0; i < len(matches); i++ {
		wartFilename := strings.ReplaceAll(matches[i], "\"", "")
		urlsToDownload[i] = fmt.Sprintf("%s/%s", currentURL, wartFilename)
	}

	return urlsToDownload, nil
}

func (a *basicArkHTTPAdapter) Download(wartLink string, date time.Time) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", wartLink, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(a.username, a.password)

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}
