package client

import (
	"fmt"
	"net"
	"time"

	"dioptra-io/ufuk-research/pkg/query"
)

type RouteTraceChunkStreamer struct {
	bufferSize int
	sqlAdapter ClickHouseSQLAdapter
	tableNames []string
}

// This represents one row for the Iris results table entry.
type RouteTraceChunk struct {
	ProbeDstAddr      net.IP      `json:"probe_dst_addr"`
	ProbeSrcAddr      net.IP      `json:"probe_src_addr"`
	ProbeDstPort      uint16      `json:"probe_dst_port"`
	ProbeSrcPort      uint16      `json:"probe_src_port"`
	ProbeProtocol     uint8       `json:"probe_protocol"`
	ProbeTTLs         []uint8     `json:"probe_ttls"`         // groupArray of probe_ttl
	CaptureTimestamps []time.Time `json:"capture_timestamps"` // groupArray of capture_timestamp
	ReplySrcAddrs     []net.IP    `json:"reply_src_addrs"`    // groupArray of reply_src_addr

	DestinationHostReplies   []uint8  `json:"destination_host_replies"`   // groupArray of destination_host_reply
	DestinationPrefixReplies []uint8  `json:"destination_prefix_replies"` // groupArray of destination_prefix_reply
	ReplyICMPTypes           []uint8  `json:"reply_icmp_types"`           // groupArray of reply_icmp_type
	ReplyICMPCodes           []uint8  `json:"reply_icmp_codes"`           // groupArray of reply_icmp_code
	ReplySizes               []uint16 `json:"reply_sizes"`                // groupArray of reply_size
	RTTs                     []uint16 `json:"rtts"`                       // groupArray of rtt
	TimeExceededReplies      []uint8  `json:"time_exceeded_replies"`      // groupArray of time_exceeded_reply
}

// Check if two of the route traces has the same  flow ids.
func (r RouteTraceChunk) FlowidEq(r2 RouteTraceChunk) bool {
	return r.ProbeDstAddr.Equal(r2.ProbeDstAddr) &&
		r.ProbeSrcAddr.Equal(r2.ProbeSrcAddr) &&
		r.ProbeSrcPort == r2.ProbeSrcPort &&
		r.ProbeDstPort == r2.ProbeDstPort &&
		r.ProbeProtocol == r2.ProbeProtocol
}

// Check if two of the route traces has the same  flow ids.
func (r RouteTraceChunk) Length() int {
	return len(r.ProbeTTLs)
}

// Check if two of the route traces has the same  flow ids.
func (r RouteTraceChunk) String() string {
	return fmt.Sprintf("src: '%v:%v' && dst: '%v:%v' && protocol: '%v'", r.ProbeSrcAddr, r.ProbeSrcPort, r.ProbeDstAddr, r.ProbeDstPort, r.ProbeProtocol)
}

func NewRouteTraceChunkSreamer(sqlAdapter ClickHouseSQLAdapter, bufferSize int, tableNames []string) StreamerChan[RouteTraceChunk] {
	return &RouteTraceChunkStreamer{
		bufferSize: bufferSize,
		sqlAdapter: sqlAdapter,
		tableNames: tableNames,
	}
}

func (p RouteTraceChunkStreamer) Stream() (<-chan RouteTraceChunk, <-chan error) {
	routeTraceCh := make(chan RouteTraceChunk, p.bufferSize)
	errCh := make(chan error)

	go func() {
		defer close(routeTraceCh)
		defer close(errCh)

		rows, err := p.sqlAdapter.Query(query.SelectRoutes(p.tableNames))
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()

		for rows.Next() {
			var routeTrace RouteTraceChunk

			if err := rows.Scan(
				&routeTrace.ProbeDstAddr,
				&routeTrace.ProbeSrcAddr,
				&routeTrace.ProbeDstPort,
				&routeTrace.ProbeSrcPort,
				&routeTrace.ProbeProtocol,
				&routeTrace.ProbeTTLs,
				&routeTrace.CaptureTimestamps,
				&routeTrace.ReplySrcAddrs,
				&routeTrace.DestinationHostReplies,
				&routeTrace.DestinationPrefixReplies,
				&routeTrace.ReplyICMPTypes,
				&routeTrace.ReplyICMPCodes,
				&routeTrace.ReplySizes,
				&routeTrace.RTTs,
				&routeTrace.TimeExceededReplies,
			); err != nil {
				errCh <- err
				return
			}

			routeTraceCh <- routeTrace
		}

		if err := rows.Err(); err != nil {
			errCh <- err
			return
		}
	}()

	return routeTraceCh, errCh
}

// Helper function to convert []interface{} to []uint8
func interfaceToUint8Slice(input []interface{}) []uint8 {
	var result []uint8
	for _, v := range input {
		result = append(result, v.(uint8))
	}
	return result
}

// Helper function to convert []interface{} to []string
func interfaceToStringSlice(input []interface{}) []string {
	var result []string
	for _, v := range input {
		result = append(result, v.(string))
	}
	return result
}
