package iris

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	defaultEndpoint = "https://api.iris.dioptra.io"
	pageLimit       = 200
)

// ── Config ───────────────────────────────────────────────────────────────────

type Config struct {
	Username string
	Password string
	Endpoint string // defaults to https://api.iris.dioptra.io
}

func (c *Config) endpoint() string {
	if c.Endpoint == "" {
		return defaultEndpoint
	}
	return strings.TrimRight(c.Endpoint, "/")
}

// ── Client ───────────────────────────────────────────────────────────────────

type IrisClient struct {
	config   Config
	http     *http.Client
	token    string
	services *ExternalServices // cached ClickHouse/S3 credentials
}

// NewIrisClient creates a new IrisClient and immediately logs in to obtain a token.
func NewIrisClient(cfg Config) (*IrisClient, error) {
	if cfg.Username == "" {
		return nil, fmt.Errorf("iris: username is required")
	}
	if cfg.Password == "" {
		return nil, fmt.Errorf("iris: password is required")
	}
	c := &IrisClient{
		config: cfg,
		http:   &http.Client{Timeout: 30 * time.Second},
	}
	if err := c.Login(); err != nil {
		return nil, fmt.Errorf("iris: initial login failed: %w", err)
	}
	return c, nil
}

// ── Auth ─────────────────────────────────────────────────────────────────────

// Login authenticates with the Iris API and stores the JWT token in memory.
func (c *IrisClient) Login() error {
	form := url.Values{}
	form.Set("username", c.config.Username)
	form.Set("password", c.config.Password)

	resp, err := c.http.PostForm(c.config.endpoint()+"/auth/jwt/login", form)
	if err != nil {
		return fmt.Errorf("iris: login request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("iris: login failed with status %d", resp.StatusCode)
	}

	var bearer BearerResponse
	if err := json.NewDecoder(resp.Body).Decode(&bearer); err != nil {
		return fmt.Errorf("iris: failed to decode login response: %w", err)
	}

	c.token = bearer.AccessToken
	return nil
}

// Logout invalidates the JWT token on the server and clears it from memory.
func (c *IrisClient) Logout() error {
	if c.token == "" {
		return fmt.Errorf("iris: not logged in")
	}

	req, err := http.NewRequest(http.MethodPost, c.config.endpoint()+"/auth/jwt/logout", nil)
	if err != nil {
		return fmt.Errorf("iris: failed to build logout request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("iris: logout request failed: %w", err)
	}
	defer resp.Body.Close()

	c.token = ""
	c.services = nil
	return nil
}

// ── Services ─────────────────────────────────────────────────────────────────

// Services returns the external service credentials (ClickHouse, S3).
// Results are cached and refreshed automatically when expired.
func (c *IrisClient) Services() (ExternalServices, error) {
	if c.services != nil && time.Now().Before(c.services.ClickHouseExpirationTime.Time) {
		return *c.services, nil
	}

	var services ExternalServices
	if err := c.get("/users/me/services", nil, &services); err != nil {
		return ExternalServices{}, fmt.Errorf("iris: failed to get services: %w", err)
	}

	c.services = &services
	return services, nil
}

// clickhouseCredentials returns valid ClickHouse credentials, refreshing if needed.
func (c *IrisClient) clickhouseCredentials() (ClickHouseCredentials, error) {
	svc, err := c.Services()
	if err != nil {
		return ClickHouseCredentials{}, err
	}
	return svc.ClickHouse, nil
}

// ── Measurement Query Builder ─────────────────────────────────────────────────

// MeasurementQueryBuilder builds and executes a filtered measurement list query.
type MeasurementQueryBuilder struct {
	client     *IrisClient
	state      *MeasurementAgentState
	from       *time.Time
	to         *time.Time
	tagPattern *regexp.Regexp
}

// Measurements returns a new MeasurementQueryBuilder.
func (c *IrisClient) Measurements() *MeasurementQueryBuilder {
	return &MeasurementQueryBuilder{client: c}
}

// State filters measurements by agent state.
func (q *MeasurementQueryBuilder) State(s MeasurementAgentState) *MeasurementQueryBuilder {
	q.state = &s
	return q
}

// Between filters measurements by creation_time range (inclusive).
func (q *MeasurementQueryBuilder) Between(from, to time.Time) *MeasurementQueryBuilder {
	q.from = &from
	q.to = &to
	return q
}

// TagContains filters measurements whose tags match the given regex pattern.
func (q *MeasurementQueryBuilder) TagContains(pattern string) *MeasurementQueryBuilder {
	q.tagPattern = regexp.MustCompile(pattern)
	return q
}

// Fetch executes the query and returns all matching measurements.
// If no state is set, it fans out over all possible states.
// If a `from` date is set, pagination stops early once results go older than
// that date, since the API returns measurements newest-first.
func (q *MeasurementQueryBuilder) Fetch() ([]MeasurementRead, error) {
	states := AllMeasurementStates
	if q.state != nil {
		states = []MeasurementAgentState{*q.state}
	}

	var all []MeasurementRead
	for _, state := range states {
		results, err := q.client.fetchAllMeasurements(state, q.from)
		if err != nil {
			return nil, err
		}
		all = append(all, results...)
	}

	return q.applyFilters(all), nil
}

// applyFilters applies in-memory filters on the fetched measurements.
func (q *MeasurementQueryBuilder) applyFilters(measurements []MeasurementRead) []MeasurementRead {
	result := make([]MeasurementRead, 0, len(measurements))
	for _, m := range measurements {
		if q.from != nil && m.CreationTime.Time.Before(*q.from) {
			continue
		}
		if q.to != nil && m.CreationTime.Time.After(*q.to) {
			continue
		}
		if q.tagPattern != nil && !matchesTagPattern(m.Tags, q.tagPattern) {
			continue
		}
		result = append(result, m)
	}
	return result
}

func matchesTagPattern(tags []string, pattern *regexp.Regexp) bool {
	for _, tag := range tags {
		if pattern.MatchString(tag) {
			return true
		}
	}
	return false
}

// fetchAllMeasurements paginates through measurements for a given state.
// If cutoff is set, it stops as soon as a result's creation_time is before
// the cutoff, exploiting the API's newest-first ordering.
func (c *IrisClient) fetchAllMeasurements(state MeasurementAgentState, cutoff *time.Time) ([]MeasurementRead, error) {
	var all []MeasurementRead
	offset := 0

	for {
		params := url.Values{}
		params.Set("state", string(state))
		params.Set("only_mine", "false")
		params.Set("limit", fmt.Sprintf("%d", pageLimit))
		params.Set("offset", fmt.Sprintf("%d", offset))

		var page Paginated[MeasurementRead]
		if err := c.get("/measurements/", params, &page); err != nil {
			return nil, fmt.Errorf("iris: failed to fetch measurements (state=%s, offset=%d): %w", state, offset, err)
		}

		for _, m := range page.Results {
			if cutoff != nil && m.CreationTime.Time.Before(*cutoff) {
				// Everything from here on is older than our cutoff — stop.
				return all, nil
			}
			all = append(all, m)
		}

		if len(all) >= page.Count || len(page.Results) < pageLimit {
			break
		}
		offset += pageLimit
	}

	return all, nil
}

// ── ClickHouse Query Builder ──────────────────────────────────────────────────

// clickhouseFormat represents a ClickHouse output format.
type clickhouseFormat string

const (
	formatRaw       clickhouseFormat = ""
	formatJSON      clickhouseFormat = "JSONEachRow"
	formatCSV       clickhouseFormat = "CSVWithNames"
	formatRowBinary clickhouseFormat = "RowBinaryWithNamesAndTypes"
)

// QueryBuilder is created by client.Query() and holds a reference to the client.
type QueryBuilder struct {
	client *IrisClient
}

// Query returns a new QueryBuilder for executing ClickHouse queries.
func (c *IrisClient) Query() *QueryBuilder {
	return &QueryBuilder{client: c}
}

// Select sets the SQL query and returns a SelectQuery ready to execute.
func (q *QueryBuilder) Select(sql string) *SelectQuery {
	return &SelectQuery{
		client: q.client,
		sql:    sql,
	}
}

// SelectQuery holds a SQL query and executes it against ClickHouse.
type SelectQuery struct {
	client *IrisClient
	sql    string
}

// Raw executes the query and returns the raw ClickHouse response body.
func (q *SelectQuery) Raw() (io.ReadCloser, error) {
	return q.execute(formatRaw)
}

// Json executes the query with FORMAT JSONEachRow and returns the response body.
func (q *SelectQuery) Json() (io.ReadCloser, error) {
	return q.execute(formatJSON)
}

// Csv executes the query with FORMAT CSVWithNames and returns the response body.
func (q *SelectQuery) Csv() (io.ReadCloser, error) {
	return q.execute(formatCSV)
}

// execute fires the query against ClickHouse with the given format appended.
func (q *SelectQuery) execute(format clickhouseFormat) (io.ReadCloser, error) {
	creds, err := q.client.clickhouseCredentials()
	if err != nil {
		return nil, fmt.Errorf("iris: failed to get clickhouse credentials: %w", err)
	}

	sql := q.sql
	if format != formatRaw {
		sql = fmt.Sprintf("%s FORMAT %s", strings.TrimRight(sql, " \t\n;"), format)
	}

	params := url.Values{}
	params.Set("query", sql)
	params.Set("database", creds.Database)
	params.Set("enable_http_compression", "1")
	params.Set("max_execution_time", "3600") // 1 hour

	u := creds.BaseURL + "?" + params.Encode()

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("iris: failed to build clickhouse request: %w", err)
	}
	req.SetBasicAuth(creds.Username, creds.Password)
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := q.client.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("iris: clickhouse request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("iris: clickhouse error (status %d): %s", resp.StatusCode, string(body))
	}

	// resp.Body is returned directly — caller is responsible for closing it.
	return resp.Body, nil
}

// ── HTTP Helpers ─────────────────────────────────────────────────────────────

// get performs an authenticated GET request and decodes the JSON response.
// On a 401 it re-logs in once and retries.
func (c *IrisClient) get(path string, params url.Values, out any) error {
	return c.getWithRetry(path, params, out, true)
}

func (c *IrisClient) getWithRetry(path string, params url.Values, out any, retry bool) error {
	u := c.config.endpoint() + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("iris: failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("iris: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized && retry {
		if err := c.Login(); err != nil {
			return fmt.Errorf("iris: re-login failed: %w", err)
		}
		return c.getWithRetry(path, params, out, false)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("iris: unexpected status %d for %s", resp.StatusCode, path)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("iris: failed to decode response: %w", err)
	}

	return nil
}
