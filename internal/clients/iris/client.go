package iris

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// IrisClient defines the interface for interacting with the Iris API
type IrisClient interface {
	// Login authenticates with the API and returns a JWT token
	Login(ctx context.Context) error

	// Get the access token
	GetAccessToken() string

	// GetCurrentUser retrieves the current authenticated user's information
	GetCurrentUser(ctx context.Context) (*IrisUser, error)

	// ListMeasurementsPage retrieves a single page of measurements
	ListMeasurementsPage(ctx context.Context, params *IrisMeasurementListParams) (*IrisMeasurementListResponse, error)

	// ListMeasurements retrieves ALL measurements by fetching all pages
	ListMeasurements(ctx context.Context, params *IrisMeasurementListParams) ([]IrisMeasurement, error)

	// GetMeasurementServices retrieves temporary credentials for accessing measurement data
	// It automatically fetches a finished measurement to get the UUID
	GetMeasurementServices(ctx context.Context) (*IrisServicesResponse, error)

	// QueryClickHouse executes a SQL query on ClickHouse and returns the response body as a stream
	// The caller is responsible for closing the returned io.ReadCloser
	QueryClickHouse(ctx context.Context, query string) (io.ReadCloser, error)
}

// irisClient implements the IrisClient interface
type irisClient struct {
	baseURL    string
	httpClient *http.Client

	// Credentials
	username string
	password string

	// Token management
	mu          sync.RWMutex
	accessToken string
	tokenExpiry time.Time

	// ClickHouse credentials management
	clickhouseCredentials *IrisClickHouseCredentials
	clickhouseExpiry      time.Time
	s3Credentials         *IrisS3Credentials
	s3Expiry              time.Time
}

var _ IrisClient = (*irisClient)(nil)

// NewIrisClient creates a new Iris API client with credentials
func NewIrisClient(baseURL, username, password string) IrisClient {
	return &irisClient{
		baseURL:  baseURL,
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Login authenticates with the API, returns an error if it fails.
func (c *irisClient) Login(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/auth/jwt/login", c.baseURL)
	// Use stored credentials
	username := c.username
	password := c.password
	// Prepare form data
	formData := url.Values{}
	formData.Set("grant_type", "password")
	formData.Set("username", username)
	formData.Set("password", password)
	formData.Set("scope", "")
	formData.Set("client_id", "string")
	formData.Set("client_secret", "string")
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(formData.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return c.handleErrorResponse(resp)
	}
	var loginResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("failed to decode login response: %w", err)
	}
	// Store token and set expiry to 1 hour from now
	c.mu.Lock()
	c.accessToken = loginResp.AccessToken
	c.tokenExpiry = time.Now().Add(1 * time.Hour)
	c.mu.Unlock()
	return nil
}

// GetAccessToken returns the current access token (useful for testing or manual token usage)
func (c *irisClient) GetAccessToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.accessToken
}

// ensureValidToken checks if the token is valid and refreshes it if needed
func (c *irisClient) ensureValidToken(ctx context.Context) error {
	c.mu.RLock()
	needsRefresh := time.Now().After(c.tokenExpiry)
	c.mu.RUnlock()

	if needsRefresh {
		return c.Login(ctx)
	}
	return nil
}

// doRequest is a helper method to make HTTP requests
func (c *irisClient) doRequest(ctx context.Context, method, url string, body any, useAuth bool) (*http.Response, error) {
	var reqBody io.Reader

	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if useAuth {
		c.mu.RLock()
		token := c.accessToken
		c.mu.RUnlock()

		if token != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}
	}

	return c.httpClient.Do(req)
}

// handleErrorResponse handles error responses based on HTTP status codes
func (c *irisClient) handleErrorResponse(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("HTTP %d: failed to read error response", resp.StatusCode)
	}

	// Create error message based on status code
	var errMsg string
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		errMsg = "authentication failed"
	case http.StatusForbidden:
		errMsg = "access forbidden"
	case http.StatusNotFound:
		errMsg = "endpoint not found"
	case http.StatusInternalServerError:
		errMsg = "internal server error"
	default:
		errMsg = "request failed"
	}

	return fmt.Errorf("HTTP %d - %s: %s", resp.StatusCode, errMsg, string(body))
}

// GetCurrentUser retrieves the current authenticated user's information
func (c *irisClient) GetCurrentUser(ctx context.Context) (*IrisUser, error) {
	// Ensure we have a valid token
	if err := c.ensureValidToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid token: %w", err)
	}

	endpoint := fmt.Sprintf("%s/users/me", c.baseURL)

	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil, true)
	if err != nil {
		return nil, fmt.Errorf("get current user request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var user IrisUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("failed to decode user response: %w", err)
	}

	return &user, nil
}

// ListMeasurements retrieves a paginated list of measurements
func (c *irisClient) ListMeasurementsPage(ctx context.Context, params *IrisMeasurementListParams) (*IrisMeasurementListResponse, error) {
	// Ensure we have a valid token
	if err := c.ensureValidToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid token: %w", err)
	}

	// Build URL with query parameters
	endpoint := fmt.Sprintf("%s/measurements/", c.baseURL)

	// Build query parameters
	queryParams := url.Values{}
	if params != nil {
		if params.State != nil {
			queryParams.Set("state", string(*params.State))
		}
		if params.Tag != nil {
			queryParams.Set("tag", string(*params.Tag))
		}
		if params.OnlyMine != nil {
			queryParams.Set("only_mine", fmt.Sprintf("%t", *params.OnlyMine))
		}
		if params.Offset != nil {
			queryParams.Set("offset", fmt.Sprintf("%d", *params.Offset))
		}
		if params.Limit != nil {
			queryParams.Set("limit", fmt.Sprintf("%d", *params.Limit))
		}
	}

	if len(queryParams) > 0 {
		endpoint = fmt.Sprintf("%s?%s", endpoint, queryParams.Encode())
	}

	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil, true)
	if err != nil {
		return nil, fmt.Errorf("list measurements request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var measurementList IrisMeasurementListResponse
	if err := json.NewDecoder(resp.Body).Decode(&measurementList); err != nil {
		return nil, fmt.Errorf("failed to decode measurements response: %w", err)
	}

	return &measurementList, nil
}

// ListMeasurements retrieves measurements matching the criteria by fetching pages If params.Limit is nil or -1, fetches
// ALL measurements Otherwise, fetches up to the specified limit params.Offset specifies where to start fetching
// (default 0)
func (c *irisClient) ListMeasurements(ctx context.Context, params *IrisMeasurementListParams) ([]IrisMeasurement, error) {
	// Determine if we should fetch all or limited
	fetchAll := params == nil || params.Limit == nil || *params.Limit == -1
	var maxResults int
	if !fetchAll {
		maxResults = *params.Limit
	}

	// Determine starting offset
	startOffset := 0
	if params != nil && params.Offset != nil {
		startOffset = *params.Offset
	}

	// Create params for pagination
	reqParams := &IrisMeasurementListParams{}
	if params != nil {
		reqParams.State = params.State
		reqParams.Tag = params.Tag
		reqParams.OnlyMine = params.OnlyMine
	}

	offset := startOffset
	limit := IRIS_PAGE_SIZE
	reqParams.Offset = &offset
	reqParams.Limit = &limit

	var allMeasurements []IrisMeasurement

	for {
		// Adjust limit for last page if we have a max limit
		if !fetchAll {
			remaining := maxResults - len(allMeasurements)
			if remaining <= 0 {
				break
			}
			if remaining < IRIS_PAGE_SIZE {
				limit = remaining
				reqParams.Limit = &limit
			}
		}

		// Fetch one page
		page, err := c.ListMeasurementsPage(ctx, reqParams)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch page at offset %d: %w", offset, err)
		}

		// Append results
		allMeasurements = append(allMeasurements, page.Results...)

		// Check if we have more pages
		if page.Next == nil || len(page.Results) == 0 {
			break
		}

		// If we've reached the limit, stop
		if !fetchAll && len(allMeasurements) >= maxResults {
			break
		}

		// Update offset for next page
		offset += len(page.Results)
		reqParams.Offset = &offset
	}

	// Trim to exact limit if needed
	if !fetchAll && len(allMeasurements) > maxResults {
		allMeasurements = allMeasurements[:maxResults]
	}

	return allMeasurements, nil
}

// GetMeasurementServices retrieves temporary credentials for accessing measurement data
// It automatically fetches a finished measurement to get the UUID
func (c *irisClient) GetMeasurementServices(ctx context.Context) (*IrisServicesResponse, error) {
	// Ensure we have a valid token
	if err := c.ensureValidToken(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid token: %w", err)
	}

	// Get a finished measurement UUID with default params
	state := MeasurementStateFinished
	onlyMine := false
	offset := 0
	limit := 1

	page, err := c.ListMeasurementsPage(ctx, &IrisMeasurementListParams{
		State:    &state,
		OnlyMine: &onlyMine,
		Offset:   &offset,
		Limit:    &limit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch measurement: %w", err)
	}

	if len(page.Results) == 0 {
		return nil, fmt.Errorf("no finished measurements found")
	}

	measurementUUID := page.Results[0].UUID

	endpoint := fmt.Sprintf("%s/users/me/services?measurement_uuid=%s", c.baseURL, measurementUUID)

	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, nil, true)
	if err != nil {
		return nil, fmt.Errorf("get measurement services request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	var services IrisServicesResponse
	if err := json.NewDecoder(resp.Body).Decode(&services); err != nil {
		return nil, fmt.Errorf("failed to decode services response: %w", err)
	}

	// Store credentials and expiry times
	c.mu.Lock()
	c.clickhouseCredentials = &services.ClickHouse
	c.clickhouseExpiry = services.ClickHouseExpiration.Time
	c.s3Credentials = &services.S3
	c.s3Expiry = services.S3Expiration.Time
	c.mu.Unlock()

	return &services, nil
}

// ensureValidClickHouseCredentials checks if ClickHouse credentials are valid and refreshes them if needed
func (c *irisClient) ensureValidClickHouseCredentials(ctx context.Context) error {
	c.mu.RLock()
	hasCredentials := c.clickhouseCredentials != nil
	needsRefresh := time.Now().After(c.clickhouseExpiry)
	c.mu.RUnlock()

	if !hasCredentials || needsRefresh {
		// Refresh credentials by calling GetMeasurementServices
		_, err := c.GetMeasurementServices(ctx)
		if err != nil {
			return fmt.Errorf("failed to refresh ClickHouse credentials: %w", err)
		}
	}

	return nil
}

// QueryClickHouse executes a SQL query on ClickHouse and returns the response body as a stream
// The caller is responsible for closing the returned io.ReadCloser
func (c *irisClient) QueryClickHouse(ctx context.Context, query string) (io.ReadCloser, error) {
	// Ensure we have valid ClickHouse credentials
	if err := c.ensureValidClickHouseCredentials(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure valid ClickHouse credentials: %w", err)
	}

	// Get credentials safely
	c.mu.RLock()
	baseURL := c.clickhouseCredentials.BaseURL
	database := c.clickhouseCredentials.Database
	username := c.clickhouseCredentials.Username
	password := c.clickhouseCredentials.Password
	c.mu.RUnlock()

	// Build ClickHouse query URL
	queryURL := fmt.Sprintf("%s/?database=%s&query=%s",
		baseURL,
		url.QueryEscape(database),
		url.QueryEscape(query))

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse request: %w", err)
	}

	// Set basic auth
	req.SetBasicAuth(username, password)
	req.Header.Set("Accept", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ClickHouse query request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("ClickHouse query failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	// Return the response body directly for streaming
	return resp.Body, nil
}
