package iris

import (
	"fmt"
	"strings"
	"time"
)

// IrisTime handles timestamps returned by the Iris API, which may omit the
// timezone suffix (e.g. "2026-06-01T16:34:34.849708" instead of RFC3339).
// When no timezone is present, UTC is assumed.
type IrisTime struct {
	time.Time
}

var irisTimeFormats = []string{
	"2006-01-02T15:04:05.999999", // no timezone
	"2006-01-02T15:04:05",        // no timezone, no micros
	time.RFC3339Nano,
	time.RFC3339,
}

func (t *IrisTime) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if s == "null" {
		t.Time = time.Time{}
		return nil
	}
	for _, format := range irisTimeFormats {
		if parsed, err := time.ParseInLocation(format, s, time.UTC); err == nil {
			t.Time = parsed
			return nil
		}
	}
	return fmt.Errorf("iris: cannot parse time %q", s)
}

func (t IrisTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Time.Format(time.RFC3339Nano) + `"`), nil
}

type BearerResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
}

type MeasurementAgentState string

const (
	StateAgentFailure MeasurementAgentState = "agent_failure"
	StateCanceled     MeasurementAgentState = "canceled"
	StateCreated      MeasurementAgentState = "created"
	StateFinished     MeasurementAgentState = "finished"
	StateOngoing      MeasurementAgentState = "ongoing"
)

var AllMeasurementStates = []MeasurementAgentState{
	StateAgentFailure,
	StateCanceled,
	StateCreated,
	StateFinished,
	StateOngoing,
}

type Tool string

const (
	ToolDiamondMiner Tool = "diamond-miner"
	ToolYarrp        Tool = "yarrp"
	ToolPing         Tool = "ping"
	ToolProbes       Tool = "probes"
)

type AgentParameters struct {
	Version             string   `json:"version"`
	Hostname            string   `json:"hostname"`
	InternalIPv4Address *string  `json:"internal_ipv4_address"`
	InternalIPv6Address *string  `json:"internal_ipv6_address"`
	ExternalIPv4Address *string  `json:"external_ipv4_address"`
	ExternalIPv6Address *string  `json:"external_ipv6_address"`
	CPUs                int      `json:"cpus"`
	Disk                float64  `json:"disk"`
	Memory              float64  `json:"memory"`
	MinTTL              int      `json:"min_ttl"`
	MaxProbingRate      int      `json:"max_probing_rate"`
	Tags                []string `json:"tags"`
}

type MeasurementAgentReadLite struct {
	AgentUUID string `json:"agent_uuid"`
}

type MeasurementAgentRead struct {
	TargetFile        string                `json:"target_file"`
	AgentUUID         string                `json:"agent_uuid"`
	AgentParameters   AgentParameters       `json:"agent_parameters"`
	ProbingStatistics map[string]any        `json:"probing_statistics"`
	State             MeasurementAgentState `json:"state"`
	BatchSize         *int                  `json:"batch_size"`
	ProbingRate       *int                  `json:"probing_rate"`
}

type MeasurementRead struct {
	UUID         string                     `json:"uuid"`
	Tool         Tool                       `json:"tool"`
	Tags         []string                   `json:"tags"`
	UserID       string                     `json:"user_id"`
	CreationTime IrisTime                   `json:"creation_time"`
	StartTime    *IrisTime                  `json:"start_time"`
	EndTime      *IrisTime                  `json:"end_time"`
	State        MeasurementAgentState      `json:"state"`
	Agents       []MeasurementAgentReadLite `json:"agents"`
}

type MeasurementReadWithAgents struct {
	UUID         string                 `json:"uuid"`
	Tool         Tool                   `json:"tool"`
	Tags         []string               `json:"tags"`
	UserID       string                 `json:"user_id"`
	CreationTime IrisTime               `json:"creation_time"`
	StartTime    *IrisTime              `json:"start_time"`
	EndTime      *IrisTime              `json:"end_time"`
	State        MeasurementAgentState  `json:"state"`
	Agents       []MeasurementAgentRead `json:"agents"`
}

type Paginated[T any] struct {
	Count    int     `json:"count"`
	Next     *string `json:"next"`
	Previous *string `json:"previous"`
	Results  []T     `json:"results"`
}

type ClickHouseCredentials struct {
	BaseURL  string `json:"base_url"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type AWSCredentials struct {
	AccessKeyID     string `json:"aws_access_key_id"`
	SecretAccessKey string `json:"aws_secret_access_key"`
	SessionToken    string `json:"aws_session_token"`
	EndpointURL     string `json:"endpoint_url"`
}

type ExternalServices struct {
	ClickHouse               ClickHouseCredentials `json:"clickhouse"`
	ClickHouseExpirationTime IrisTime              `json:"clickhouse_expiration_time"`
	S3                       AWSCredentials        `json:"s3"`
	S3ExpirationTime         IrisTime              `json:"s3_expiration_time"`
}

type IrisTableKind string

const (
	TableKindResults  IrisTableKind = "results"
	TableKindPrefixes IrisTableKind = "prefixes"
	TableKindLinks    IrisTableKind = "links"
	TableKindProbes   IrisTableKind = "probes"
)

var AllTableKinds = []IrisTableKind{
	TableKindResults,
	TableKindPrefixes,
	TableKindLinks,
	TableKindProbes,
}

type IrisTable struct {
	Kind            IrisTableKind
	TableName       string
	MeasurementUUID string
	AgentUUID       string
	CreationTime    IrisTime
}

type IrisTableGroup struct {
	MeasurementUUID string
	AgentUUID       string
	CreationTime    IrisTime
	Results         IrisTable
	Prefixes        IrisTable
	Links           IrisTable
	Probes          IrisTable
}

// uuidToTablePart replaces "-" with "_" in a UUID for use in table names.
func uuidToTablePart(uuid string) string {
	return strings.ReplaceAll(uuid, "-", "_")
}

// tableName builds a table name from kind, measurement UUID and agent UUID.
func tableName(kind IrisTableKind, measurementUUID, agentUUID string) string {
	return fmt.Sprintf("%s__%s__%s",
		kind,
		uuidToTablePart(measurementUUID),
		uuidToTablePart(agentUUID),
	)
}

// NewIrisTableGroup constructs an IrisTableGroup for a given measurement and agent.
func NewIrisTableGroup(measurementUUID, agentUUID string, creationTime IrisTime) IrisTableGroup {
	makeTable := func(kind IrisTableKind) IrisTable {
		return IrisTable{
			Kind:            kind,
			TableName:       tableName(kind, measurementUUID, agentUUID),
			MeasurementUUID: measurementUUID,
			AgentUUID:       agentUUID,
			CreationTime:    creationTime,
		}
	}

	return IrisTableGroup{
		MeasurementUUID: measurementUUID,
		AgentUUID:       agentUUID,
		CreationTime:    creationTime,
		Results:         makeTable(TableKindResults),
		Prefixes:        makeTable(TableKindPrefixes),
		Links:           makeTable(TableKindLinks),
		Probes:          makeTable(TableKindProbes),
	}
}

// TableGroupsForMeasurement derives all IrisTableGroups from a MeasurementRead.
func TableGroupsForMeasurement(m MeasurementRead) []IrisTableGroup {
	groups := make([]IrisTableGroup, 0, len(m.Agents))
	for _, agent := range m.Agents {
		groups = append(groups, NewIrisTableGroup(m.UUID, agent.AgentUUID, m.CreationTime))
	}
	return groups
}
