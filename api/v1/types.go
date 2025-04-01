package v1

import (
	"errors"
	"strings"
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

// LOL
// type ArkCycle struct {
// 	Date time.Time `json:"date"`
// 	URL  string    `json:"url"`
// }
//
// type ArkWartFile struct {
// 	Date time.Time `json:"date"`
// 	Name string    `json:"name"`
// 	URL  string    `json:"url"`
// }
//
// type RouteNextHop struct {
// 	// Most important data.
// 	IPAddr   net.IP
// 	NextAddr net.IP
//
// 	// Additionalt metadata.
// 	FirstCaptureTimestamp time.Time
//
// 	// Flowid
// 	ProbeSrcAddr  net.IP
// 	ProbeDstAddr  net.IP // Destination prefix can be found from this
// 	ProbeSrcPort  uint16
// 	ProbeDstPort  uint16
// 	ProbeProtocol uint8
//
// 	// These are the other info might me useful with the next hop row
// 	IsDestinationHostReply   uint8
// 	IsDestinationPrefixReply uint8
// 	ReplyICMPType            uint8
// 	ReplyICMPCode            uint8
// 	ReplySize                uint16
// 	RTT                      uint16
// 	TimeExceededReply        uint8
// }
//
// // This represents one row for the Iris results table entry.
// type RouteTraceChunk struct {
// 	ProbeDstAddr      net.IP      `json:"probe_dst_addr"`
// 	ProbeSrcAddr      net.IP      `json:"probe_src_addr"`
// 	ProbeDstPort      uint16      `json:"probe_dst_port"`
// 	ProbeSrcPort      uint16      `json:"probe_src_port"`
// 	ProbeProtocol     uint8       `json:"probe_protocol"`
// 	ProbeTTLs         []uint8     `json:"probe_ttls"`         // groupArray of probe_ttl
// 	CaptureTimestamps []time.Time `json:"capture_timestamps"` // groupArray of capture_timestamp
// 	ReplySrcAddrs     []net.IP    `json:"reply_src_addrs"`    // groupArray of reply_src_addr
//
// 	DestinationHostReplies   []uint8  `json:"destination_host_replies"`   // groupArray of destination_host_reply
// 	DestinationPrefixReplies []uint8  `json:"destination_prefix_replies"` // groupArray of destination_prefix_reply
// 	ReplyICMPTypes           []uint8  `json:"reply_icmp_types"`           // groupArray of reply_icmp_type
// 	ReplyICMPCodes           []uint8  `json:"reply_icmp_codes"`           // groupArray of reply_icmp_code
// 	ReplySizes               []uint16 `json:"reply_sizes"`                // groupArray of reply_size
// 	RTTs                     []uint16 `json:"rtts"`                       // groupArray of rtt
// 	TimeExceededReplies      []uint8  `json:"time_exceeded_replies"`      // groupArray of time_exceeded_reply
// }
//
// // This represents one row for the Iris results table entry.
// type ProbeRecord struct {
// 	CaptureTimestamp time.Time
// 	ProbeProtocol    uint8
// 	ProbeSrcAddr     net.IP
// 	ProbeDstAddr     net.IP
// 	ProbeSrcPort     uint16
// 	ProbeDstPort     uint16
// 	ProbeTTL         uint8
// 	QuotedTTL        uint8
// 	ReplySrcAddr     net.IP
// 	ReplyProtocol    uint8
// 	ReplyICMPType    uint8
// 	ReplyICMPCode    uint8
// 	ReplyTTL         uint8
// 	ReplySize        uint16
// 	ReplyMPLSLabels  []struct {
// 		Label         uint32
// 		Exp           uint8
// 		BottomOfStack uint8
// 		TTL           uint8
// 	}
// 	RTT   uint16
// 	Round uint8
// }
//
// func ArkCycleFromTime(t time.Time) *ArkCycle {
// 	return &ArkCycle{
// 		Date: t,
// 		URL:  ArkCycleURL(t),
// 	}
// }
//
// func ArkWartFromTime(t time.Time, name string) *ArkWartFile {
// 	return &ArkWartFile{
// 		Date: t,
// 		URL:  ArkWartURL(t, name),
// 	}
// }
//
// func ArkCycleURL(t time.Time) string {
// 	dateString := util.TimeString(t)
// 	generatedURL := fmt.Sprintf("%s/%v/cycle-%s", config.ArkIPv4DatabaseBaseUrl, t.Year(), dateString)
// 	return generatedURL
// }
//
// func ArkWartURL(t time.Time, name string) string {
// 	dateString := ArkCycleURL(t)
// 	return fmt.Sprintf("%s/%s", dateString, name)
// }
//
// // Check if two of the route traces has the same  flow ids.
// func (r RouteTraceChunk) FlowidEq(r2 RouteTraceChunk) bool {
// 	return r.ProbeDstAddr.Equal(r2.ProbeDstAddr) &&
// 		r.ProbeSrcAddr.Equal(r2.ProbeSrcAddr) &&
// 		r.ProbeSrcPort == r2.ProbeSrcPort &&
// 		r.ProbeDstPort == r2.ProbeDstPort &&
// 		r.ProbeProtocol == r2.ProbeProtocol
// }
//
// // Check if two of the route traces has the same  flow ids.
// func (r RouteTraceChunk) Length() int {
// 	return len(r.ProbeTTLs)
// }
//
// // Check if two of the route traces has the same  flow ids.
// func (r RouteTraceChunk) String() string {
// 	return fmt.Sprintf("src: '%v:%v' && dst: '%v:%v' && protocol: '%v'", r.ProbeSrcAddr, r.ProbeSrcPort, r.ProbeDstAddr, r.ProbeDstPort, r.ProbeProtocol)
// }
