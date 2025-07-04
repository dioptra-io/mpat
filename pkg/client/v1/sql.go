package v1

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	"github.com/dioptra-io/ufuk-research/pkg/query"
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
	if scheme == "tcp" {
		host = strings.ReplaceAll(host, ":9000", ":8123")
		scheme = "http"
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
	var one int
	if err := a.QueryRow(query.Select1()).Scan(&one); err != nil || one != 1 {
		return err
	}
	return nil
}

func (a *SQLClient) GetTableInfoFromTableName(tablesToCheck []apiv1.TableName) ([]apiv1.ResultsTableInfo, error) {
	tableNames := make([]string, 0, len(tablesToCheck))
	for i := 0; i < len(tablesToCheck); i++ {
		tableNames = append(tableNames, string(tablesToCheck[i]))
	}
	return a.GetTableInfo(tableNames)
}

func (a *SQLClient) GetTableInfo(tablesToCheck []string) ([]apiv1.ResultsTableInfo, error) {
	infoToReturn := make([]apiv1.ResultsTableInfo, len(tablesToCheck))

	for i, tableName := range tablesToCheck { // this can be optimized bu one query
		info := apiv1.ResultsTableInfo{
			TableName:   tableName,
			Exists:      false, // start with exists false
			NumRows:     0,
			NumBytes:    0,
			ColumnNames: []string{}, // for now this is not supported
		}
		infoToReturn[i] = info
	}

	rows, err := a.Query(query.SelectTablesInfo(a.database, tablesToCheck))
	if err != nil {
		if err == sql.ErrNoRows {
			return infoToReturn, nil
		}
		return nil, err
	}

	current := 0
	for rows.Next() {
		var scannedName string
		var scannedNumRows uint64
		var scannedNumBytes uint64

		if err := rows.Scan(&scannedName, &scannedNumRows, &scannedNumBytes); err != nil {
			return nil, err
		}

		// iterate until it is a match
		for current < len(infoToReturn) && scannedName != infoToReturn[current].TableName {
			current++
		}

		if current >= len(infoToReturn) {
			return nil, errors.New("there are more elements in query return than we have, connect to maintainer if this happens")
		}

		infoToReturn[current].Exists = true
		infoToReturn[current].NumRows = scannedNumRows
		infoToReturn[current].NumBytes = scannedNumBytes

		current++
	}

	return infoToReturn, nil
}

func (a *SQLClient) DropTableIfNotExists(tableName string) error {
	_, err := a.Exec(query.DropTable(tableName, true))
	if err != nil {
		return err
	}
	return nil
}

func (a *SQLClient) TruncateTableIfNotExists(tableName string) error {
	_, err := a.Exec(query.TruncateTable(tableName, true))
	if err != nil {
		return err
	}
	return nil
}

func (a *SQLClient) CreateResultsTableIfNotExists(tableName string) error {
	_, err := a.Exec(query.CreateResultsTable(tableName, true))
	if err != nil {
		return err
	}
	return nil
}

func (a *SQLClient) CreateRoutesTableIfNotExists(tableName string) error {
	_, err := a.Exec(query.CreateRoutesTable(tableName, true))
	if err != nil {
		return err
	}
	return nil
}

func (a *SQLClient) UploadRouteInfos(tableName string, routeInfos []*apiv1.RouteHop) error {
	tx, err := a.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query.InsertRoutes(tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range routeInfos {
		if _, err := stmt.Exec(
			record.IPAddr,
			record.NextAddr,
			record.FirstCaptureTimestamp,
			record.ProbeSrcAddr,
			record.ProbeDstAddr,
			record.ProbeSrcPort,
			record.ProbeDstPort,
			record.ProbeProtocol,
			record.IsDestinationHostReply,
			record.IsDestinationPrefixReply,
			record.ReplyICMPType,
			record.ReplyICMPCode,
			record.ReplySize,
			record.RTT,
			record.TimeExceededReply); err != nil {
			return err
		}
	}

	return tx.Commit()
}
