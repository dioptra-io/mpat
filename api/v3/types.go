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
	ProbeDstPrefix         net.IP      `json:"probe_dst_prefix"         mpat:"no_insert"` // Computed
	ReplySrcPrefix         net.IP      `json:"reply_src_prefix"         mpat:"no_insert"` // Computed
	PrivateProbeDstPrefix  uint8       `json:"private_probe_dst_prefix" mpat:"no_insert"` // Computed
	PrivateReplySrcAddr    uint8       `json:"private_reply_src_addr"   mpat:"no_insert"` // Computed
	DestinationHostReply   uint8       `json:"destination_host_reply"   mpat:"no_insert"` // Computed
	DestinationPrefixReply uint8       `json:"destination_prefix_reply" mpat:"no_insert"` // Computed
	ValidProbeProtocol     uint8       `json:"valid_probe_protocol"     mpat:"no_insert"` // Computed
	TimeExceededReply      uint8       `json:"time_exceeded_reply"      mpat:"no_insert"` // Computed
}

// Ark specific stuff
type WartFile struct {
	URL string
}

type IrisctlMeasurementResponse struct {
	Count    int                  `json:"count"`
	Next     string               `json:"next"`
	Previous *string              `json:"previous"`
	Results  []IrisctlMeasurement `json:"results"`
}

type IrisctlMeasurement struct {
	Tool         string               `json:"tool"`
	Tags         []string             `json:"tags"`
	UUID         string               `json:"uuid"`
	UserID       string               `json:"user_id"`
	CreationTime *IrisAPITime         `json:"creation_time"`
	StartTime    *IrisAPITime         `json:"start_time"`
	EndTime      *IrisAPITime         `json:"end_time"`
	State        string               `json:"state"`
	Agents       []IrisctlAgentRecord `json:"agents"`
}

type IrisctlAgentRecord struct {
	AgentUUID string `json:"agent_uuid"`
}

type IrisAPITime struct {
	time.Time
}

const localLayout = "2006-01-02T15:04:05.999999"

func (lt *IrisAPITime) UnmarshalJSON(b []byte) error {
	s := string(b)
	// Remove quotes
	s = s[1 : len(s)-1]
	t, err := time.Parse(localLayout, s)
	if err != nil {
		return err
	}
	lt.Time = t
	return nil
}

type GrouppedForwardingDecisionResultsRow struct {
	// Info used in forwarding decision computation
	// CaptureTimestamps []time.Time `json:"capture_timestamps" mpat:"group_uniq_array"`
	ProbeTTLs     []uint8  `json:"probe_ttls"      mpat:"group_uniq_array"`
	ReplySrcAddrs []net.IP `json:"reply_src_addrs" mpat:"group_uniq_array"`
	Rounds        []uint8  `json:"rounds"          mpat:"group_uniq_array"`

	// FlowID
	ProbeProtocol  uint8  `json:"probe_protocol"`
	ProbeSrcAddr   net.IP `json:"probe_src_addr"`
	ProbeDstPrefix net.IP `json:"probe_dst_prefix"`
	ProbeDstAddr   net.IP `json:"probe_dst_addr"`
	ProbeSrcPort   uint16 `json:"probe_src_port"`
	ProbeDstPort   uint16 `json:"probe_dst_port"`

	// Other fields that are not used currently
	// QuotedTTL              uint8       `json:"quoted_ttl"`
	// ReplyProtocol          uint8       `json:"reply_protocol"`
	// ReplyICMPType          uint8       `json:"reply_icmp_type"`
	// ReplyICMPCode          uint8       `json:"reply_icmp_code"`
	// ReplyTTL               uint8       `json:"reply_ttl"`
	// ReplySize              uint16      `json:"reply_size"`
	// ReplyMPLSLabels        interface{} `json:"reply_mpls_labels"`
	// RTT                    uint16      `json:"rtt"`
	// ProbeDstPrefix         net.IP      `json:"probe_dst_prefix"         mpat:"no_insert"` // Computed
	// ReplySrcPrefix         net.IP      `json:"reply_src_prefix"         mpat:"no_insert"` // Computed
	// PrivateProbeDstPrefix  uint8       `json:"private_probe_dst_prefix" mpat:"no_insert"` // Computed
	// PrivateReplySrcAddr    uint8       `json:"private_reply_src_addr"   mpat:"no_insert"` // Computed
	// DestinationHostReply   uint8       `json:"destination_host_reply"   mpat:"no_insert"` // Computed
	// DestinationPrefixReply uint8       `json:"destination_prefix_reply" mpat:"no_insert"` // Computed
	// ValidProbeProtocol     uint8       `json:"valid_probe_protocol"     mpat:"no_insert"` // Computed
	// TimeExceededReply      uint8       `json:"time_exceeded_reply"      mpat:"no_insert"` // Computed
}

type ForwardingDecisionRow struct {
	// Info used in forwarding decision computation
	// CaptureTimestamp time.Time `json:"capture_timestamp"` // not a good idea to use this in the analysis
	NearRound    uint8  `json:"near_round"`
	NearAddr     net.IP `json:"near_addr"`
	NearProbeTTL uint8  `json:"near_probe_ttl"`
	FarRound     uint8  `json:"far_round"`
	FarAddr      net.IP `json:"far_addr"`
	FarProbeTTL  uint8  `json:"far_probe_ttl"`

	// FlowID
	ProbeProtocol  uint8  `json:"probe_protocol"`
	ProbeSrcAddr   net.IP `json:"probe_src_addr"`
	ProbeDstPrefix net.IP `json:"probe_dst_prefix"`
	ProbeDstAddr   net.IP `json:"probe_dst_addr"`
	ProbeSrcPort   uint16 `json:"probe_src_port"`
	ProbeDstPort   uint16 `json:"probe_dst_port"`
}
