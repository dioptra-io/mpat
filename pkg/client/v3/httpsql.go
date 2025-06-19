package v3

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type HTTPSQLClient struct {
	basicToken string
	parsedUrl  *url.URL
	Database   string
}

func NewHTTPSQLClient(dsn string) (*HTTPSQLClient, error) {
	parsedUrl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	username := parsedUrl.User.Username()
	password, _ := parsedUrl.User.Password()
	dbname := parsedUrl.Query().Get("database")

	q := parsedUrl.Query()

	if dbname == "" {
		dbname = "default"
		q.Add("database", dbname)
	}
	q.Add("max_execution_time", "0")
	q.Add("receive_timeout", "360000")
	q.Add("send_timeout", "360000")
	q.Add("http_receive_timeout", "360000")
	q.Add("default_format", config.DefaultHTTPSQLClientFormat)
	parsedUrl.RawQuery = q.Encode()

	basicToken := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))

	c := &HTTPSQLClient{
		basicToken: basicToken,
		parsedUrl:  parsedUrl,
		Database:   dbname,
	}

	return c, nil
}

func (c *HTTPSQLClient) Download(query string) (io.ReadCloser, error) {
	body := strings.NewReader(query)

	req, err := http.NewRequest("POST", c.parsedUrl.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", c.basicToken)
	// req.Header.Set("Accept-Encoding", "gzip") // couldn't solve the compression

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("server returned status %s", resp.Status)
	}

	return resp.Body, nil
}

func (c *HTTPSQLClient) Upload(query string, body io.Reader) (string, error) {
	newUrl := *c.parsedUrl
	q := newUrl.Query()
	q.Set("query", query)
	newUrl.RawQuery = q.Encode()

	req, err := http.NewRequest("POST", newUrl.String(), body)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", c.basicToken)
	// req.Header.Set("Content-Encoding", "gzip") // couldn't solve the compression

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("server returned status %s: %s", resp.Status, string(b))
	}

	return string(b), nil
}
