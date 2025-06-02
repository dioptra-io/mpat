package v3

import (
	"net"
	"time"
)

// This represents all the api objects that have a correspondance in ClickHouse.
type Creater interface {
	// Return the creation
	CreateQuery() (string, error)
}

// probe_protocols
const (
	ProbeProtocolICMP   = 1
	ProbeProtocolUDP    = 17
	ProbeProtocolICMPv6 = 58
)

// IrisResultsRow represents a single entry in the results table.
type IrisResultsRow struct {
	CaptureTimestamp       time.Time   `json:"capture_timestamp"`
	ProbeProtocol          uint8       `json:"probe_protocol"`
	ProbeSrcAddr           net.IP      `json:"probe_src_addr"`
	ProbeDstAddr           net.IP      `json:"probe_dst_addr"`
	ProbeSrcPort           uint16      `json:"probe_src_port"`
	ProbeDstPort           uint16      `json:"probe_dst_port"`
	ProbeTTL               uint8       `json:"probe_ttl"`
	QuotedTTL              uint8       `json:"quoted_ttl"`
	ReplySrcAddr           net.IP      `json:"reply_src_addr"`
	ReplyProtocol          uint8       `json:"reply_protocol"`
	ReplyICMPType          uint8       `json:"reply_icmp_type"`
	ReplyICMPCode          uint8       `json:"reply_icmp_code"`
	ReplyTTL               uint8       `json:"reply_ttl"`
	ReplySize              uint16      `json:"reply_size"`
	ReplyMPLSLabels        interface{} `json:"reply_mpls_labels"`
	RTT                    uint16      `json:"rtt"`
	Round                  uint8       `json:"round"`
	ProbeDstPrefix         net.IP      `json:"probe_dst_prefix"         mpat:"no-insert"` // Computed
	ReplySrcPrefix         net.IP      `json:"reply_src_prefix"         mpat:"no-insert"` // Computed
	PrivateProbeDstPrefix  uint8       `json:"private_probe_dst_prefix" mpat:"no-insert"` // Computed
	PrivateReplySrcAddr    uint8       `json:"private_reply_src_addr"   mpat:"no-insert"` // Computed
	DestinationHostReply   uint8       `json:"destination_host_reply"   mpat:"no-insert"` // Computed
	DestinationPrefixReply uint8       `json:"destination_prefix_reply" mpat:"no-insert"` // Computed
	ValidProbeProtocol     uint8       `json:"valid_probe_protocol"     mpat:"no-insert"` // Computed
	TimeExceededReply      uint8       `json:"time_exceeded_reply"      mpat:"no-insert"` // Computed
}

// Ark specific stuff
type WartFile struct {
	URL string
}
