package iris

import (
	"fmt"
	"time"
)

type IrisMeasurementTag string

const (
	IRIS_IPV4_TAG IrisMeasurementTag = "zeph-gcp-daily.json"
	IRIS_IPV6_TAG IrisMeasurementTag = "ipv6-hitlist.json"
	IRIS_ALL_TAG  IrisMeasurementTag = ""
)

const IRIS_PAGE_SIZE = 200

// LoginResponse represents the response from the login endpoint
type LoginResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"` // "bearer"
}

// LoginRequest represents the login request payload
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// IrisUser represents a user in the Iris system
type IrisUser struct {
	ID               string     `json:"id"`
	Email            string     `json:"email"`
	IsActive         bool       `json:"is_active"`
	IsSuperuser      bool       `json:"is_superuser"`
	IsVerified       bool       `json:"is_verified"`
	Firstname        string     `json:"firstname"`
	Lastname         string     `json:"lastname"`
	ProbingEnabled   bool       `json:"probing_enabled"`
	ProbingLimit     int        `json:"probing_limit"`
	AllowTagReserved bool       `json:"allow_tag_reserved"`
	AllowTagPublic   bool       `json:"allow_tag_public"`
	CreationTime     CustomTime `json:"creation_time"`
}

// IrisMeasurementState represents the state of a measurement
type IrisMeasurementState string

const (
	MeasurementStateAgentFailure IrisMeasurementState = "agent_failure"
	MeasurementStateCancelled    IrisMeasurementState = "cancelled"
	MeasurementStateCreated      IrisMeasurementState = "created"
	MeasurementStateOngoing      IrisMeasurementState = "ongoing"
	MeasurementStateFinished     IrisMeasurementState = "finished"
)

// IrisAgent represents an agent in a measurement
type IrisAgent struct {
	AgentUUID string `json:"agent_uuid"`
}

// IrisMeasurement represents a measurement in the Iris system
type IrisMeasurement struct {
	Tool         string               `json:"tool"`
	Tags         []string             `json:"tags"`
	UUID         string               `json:"uuid"`
	UserID       string               `json:"user_id"`
	CreationTime CustomTime           `json:"creation_time"`
	StartTime    CustomTime           `json:"start_time"`
	EndTime      CustomTime           `json:"end_time"`
	State        IrisMeasurementState `json:"state"`
	Agents       []IrisAgent          `json:"agents"`
}

// IrisMeasurementListResponse represents a paginated list of measurements
type IrisMeasurementListResponse struct {
	Count    int               `json:"count"`
	Next     *string           `json:"next"`     // Pointer because it can be null
	Previous *string           `json:"previous"` // Pointer because it can be null
	Results  []IrisMeasurement `json:"results"`
}

// IrisMeasurementListParams represents query parameters for listing measurements
type IrisMeasurementListParams struct {
	State    *IrisMeasurementState // Optional
	Tag      *IrisMeasurementTag   // Optional
	OnlyMine *bool                 // Optional
	Offset   *int                  // Optional
	Limit    *int                  // Optional
}

// IrisClickHouseCredentials represents ClickHouse database credentials
type IrisClickHouseCredentials struct {
	BaseURL  string `json:"base_url"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// IrisS3Credentials represents S3 storage credentials
type IrisS3Credentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	AWSSessionToken    string `json:"aws_session_token"`
	EndpointURL        string `json:"endpoint_url"`
}

// IrisServicesResponse represents credentials for accessing measurement data
type IrisServicesResponse struct {
	ClickHouse           IrisClickHouseCredentials `json:"clickhouse"`
	ClickHouseExpiration CustomTime                `json:"clickhouse_expiration_time"`
	S3                   IrisS3Credentials         `json:"s3"`
	S3Expiration         CustomTime                `json:"s3_expiration_time"`
}

// CustomTime handles timestamps with or without timezone information
type CustomTime struct {
	time.Time
}

// UnmarshalJSON implements custom JSON unmarshaling for timestamps
func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	// Remove quotes
	s = s[1 : len(s)-1]

	// Try parsing with timezone (RFC3339)
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		ct.Time = t
		return nil
	}

	// Try parsing with microseconds and timezone
	t, err = time.Parse("2006-01-02T15:04:05.999999Z", s)
	if err == nil {
		ct.Time = t
		return nil
	}

	// Try parsing with microseconds (no timezone)
	t, err = time.Parse("2006-01-02T15:04:05.999999", s)
	if err == nil {
		ct.Time = t
		return nil
	}

	// Try without microseconds (no timezone)
	t, err = time.Parse("2006-01-02T15:04:05", s)
	if err == nil {
		ct.Time = t
		return nil
	}

	return fmt.Errorf("unable to parse time: %s", s)
}
