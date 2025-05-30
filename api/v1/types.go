package v1

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ResultsTableInfo struct {
	TableName   string
	Exists      bool
	NumRows     uint64
	NumBytes    uint64
	ColumnNames []string
}

// There is a convention of the table names, this package manages them.
// Iris Results Table: results__<uuid with underscore>__<uuid with underscore>
// Iris Routes Table:  routes__<uuid with underscore>__<uuid with underscore>
// Ark Results Table:  ark_results__cycle<yearmonthdate>
// Ark Routes Table:   ark_routes__cycle<yearmonthdate>

var ErrUnknownType = errors.New("given type is an unknown table type")

type (
	TableType     string // currently results or routes
	TablePlatform string // currently ark or iris
	TableName     string // name of the table
)

const (
	UnknownTable TableType = "unknown"
	ResultsTable TableType = "results"
	RoutesTable  TableType = "routes"
)

const (
	UnknownPlatform TablePlatform = "unknown"
	IrisPlatform    TablePlatform = "iris"
	ArkPlatform     TablePlatform = "ark"
)

func (t TableName) Type() TableType {
	name := string(t)
	if strings.HasPrefix(name, "results__") {
		return ResultsTable
	} else if strings.HasPrefix(name, "routes__") {
		return RoutesTable
	}
	return UnknownTable
}

func (t TableName) Platform() TablePlatform {
	name := string(t)
	if strings.HasPrefix(name, "ark_") {
		return ArkPlatform
	} else if len(strings.Split(name, "__")) == 3 { // iris tables does not start with iris
		return IrisPlatform
	}
	return UnknownPlatform
}

func (t TableName) Convert(to TableType) (TableName, error) {
	name := string(t)
	switch t.Type() {
	case ResultsTable, RoutesTable:
		return TableName(strings.Replace(name, string(t.Type()), string(to), 1)), nil
	default:
		return "", ErrUnknownType
	}
}

// This representes the returning row of a route trace
type RouteTrace struct {
	ProbeDstAddr             net.IP      `json:"probe_dst_addr"`
	ProbeSrcAddr             net.IP      `json:"probe_src_addr"`
	ProbeDstPort             uint16      `json:"probe_dst_port"`
	ProbeSrcPort             uint16      `json:"probe_src_port"`
	ProbeProtocol            uint8       `json:"probe_protocol"`
	ProbeTTLs                []uint8     `json:"probe_ttls"`                 // groupArray of probe_ttl
	CaptureTimestamps        []time.Time `json:"capture_timestamps"`         // groupArray of capture_timestamp
	ReplySrcAddrs            []net.IP    `json:"reply_src_addrs"`            // groupArray of reply_src_addr
	DestinationHostReplies   []uint8     `json:"destination_host_replies"`   // groupArray of destination_host_reply
	DestinationPrefixReplies []uint8     `json:"destination_prefix_replies"` // groupArray of destination_prefix_reply
	ReplyICMPTypes           []uint8     `json:"reply_icmp_types"`           // groupArray of reply_icmp_type
	ReplyICMPCodes           []uint8     `json:"reply_icmp_codes"`           // groupArray of reply_icmp_code
	ReplySizes               []uint16    `json:"reply_sizes"`                // groupArray of reply_size
	RTTs                     []uint16    `json:"rtts"`                       // groupArray of rtt
	TimeExceededReplies      []uint8     `json:"time_exceeded_replies"`      // groupArray of time_exceeded_reply
}

func (r *RouteTrace) Scan(rows *sql.Rows) error {
	if err := rows.Scan(
		&r.ProbeDstAddr,
		&r.ProbeSrcAddr,
		&r.ProbeDstPort,
		&r.ProbeSrcPort,
		&r.ProbeProtocol,
		&r.ProbeTTLs,
		&r.CaptureTimestamps,
		&r.ReplySrcAddrs,
		&r.DestinationHostReplies,
		&r.DestinationPrefixReplies,
		&r.ReplyICMPTypes,
		&r.ReplyICMPCodes,
		&r.ReplySizes,
		&r.RTTs,
		&r.TimeExceededReplies,
	); err != nil {
		return err
	}
	return nil
}

func (r *RouteTrace) Length() int {
	return len(r.ProbeTTLs)
}

type RouteHop struct {
	// Most important data.
	IPAddr   net.IP
	NextAddr net.IP

	// Additionalt metadata.
	FirstCaptureTimestamp time.Time

	// Flowid
	ProbeSrcAddr  net.IP
	ProbeDstAddr  net.IP
	ProbeSrcPort  uint16
	ProbeDstPort  uint16
	ProbeProtocol uint8

	// These are the other info might me useful with the next hop row
	IsDestinationHostReply   uint8
	IsDestinationPrefixReply uint8
	ReplyICMPType            uint8
	ReplyICMPCode            uint8
	ReplySize                uint16
	RTT                      uint16
	TimeExceededReply        uint8
}

type Date struct {
	Date time.Time
}

func ParseArkDate(date string) (Date, error) {
	layout := "2006-01-02"
	var zero Date
	parsedTime, err := time.Parse(layout, date)
	if err != nil {
		return zero, nil
	}
	return Date{
		Date: parsedTime,
	}, nil
}

func (d *Date) ToArkTableName() TableName {
	return TableName(fmt.Sprintf("ark_results__cycle%d%02d%02d", d.Date.Year(), d.Date.Month(), d.Date.Day()))
}

func (d *Date) ToCycleString() string {
	return fmt.Sprintf("cycle-%d%02d%02d", d.Date.Year(), d.Date.Month(), d.Date.Day())
}

type ArkCredentials struct {
	Username string
	Pasword  string
}

func ParseArkCredentials(cred string) (ArkCredentials, error) {
	if !strings.Contains(cred, ":") {
		return ArkCredentials{}, fmt.Errorf("given credential format is not supported: '%s'", cred)
	}
	split := strings.Split(cred, ":")
	if len(split) != 2 {
		return ArkCredentials{}, fmt.Errorf("given credential format is not supported: '%s'", cred)
	}

	return ArkCredentials{
		Username: split[0],
		Pasword:  split[1],
	}, nil
}

type ArkCycle struct {
	Date Date
}

func (c *ArkCycle) GetURL() string {
	return fmt.Sprintf("%s/%d/%s", config.DefaultArkIPv4DatabaseBaseUrl, c.Date.Date.Year(), c.Date.ToCycleString())
}

type WartFile struct {
	URL string
}

// This corresponds to a tow in the results table, without the materialized columns.
type ResultsTableRow struct {
	CaptureTimestamp time.Time
	ProbeProtocol    uint8
	ProbeSrcAddr     string
	ProbeDstAddr     string
	ProbeSrcPort     uint16
	ProbeDstPort     uint16
	ProbeTTL         uint8
	QuotedTTL        uint8
	ReplySrcAddr     string
	ReplyProtocol    uint8
	ReplyICMPType    uint8
	ReplyICMPCode    uint8
	ReplyTTL         uint8
	ReplySize        uint16
	ReplyMPLSLabels  [][4]uint32 // represented as slice of 4-element tuples
	RTT              uint16
	Round            uint8
}
