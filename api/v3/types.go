package v3

import (
	"net"
	"time"
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
	ReplyMPLSLabels        interface{} `json:"reply_mpls_labels"` // we don't care really
	RTT                    uint16      `json:"rtt"`
	Round                  uint8       `json:"round"`
	ProbeDstPrefix         net.IP      `json:"probe_dst_prefix"`         // Computed
	ReplySrcPrefix         net.IP      `json:"reply_src_prefix"`         // Computed
	PrivateProbeDstPrefix  uint8       `json:"private_probe_dst_prefix"` // Computed
	PrivateReplySrcAddr    uint8       `json:"private_reply_src_addr"`   // Computed
	DestinationHostReply   uint8       `json:"destination_host_reply"`   // Computed
	DestinationPrefixReply uint8       `json:"destination_prefix_reply"` // Computed
	ValidProbeProtocol     uint8       `json:"valid_probe_protocol"`     // Computed
	TimeExceededReply      uint8       `json:"time_exceeded_reply"`      // Computed
}
