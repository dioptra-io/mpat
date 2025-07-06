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
	// Arrays from groupArray()
	ProbeTTLs     []uint8  `json:"probe_ttls"      mpat:"group_array"`
	ReplySrcAddrs []net.IP `json:"reply_src_addrs" mpat:"group_array"`
	// Rounds        []uint8  `json:"rounds"          mpat:"group_array"`
	ProbeDstAddrs []net.IP `json:"probe_dst_addrs" mpat:"group_array"`
	FlowHashes    []uint64 `json:"flowhashes"      mpat:"group_array"`

	// Grouping keys
	DistinctFlowhashes uint64 `json:"num_distinct_flowhashes"`
	ProbeSrcAddr       net.IP `json:"probe_src_addr"`
	ProbeDstPrefix     net.IP `json:"probe_dst_prefix"`
}

type ForwardingDecisionRow struct {
	// Info used in forwarding decision computation
	// CaptureTimestamp time.Time `json:"capture_timestamp"` // not a good idea to use this in the analysis
	NearAddr     net.IP `json:"near_addr"`
	FarAddr      net.IP `json:"far_addr"`
	NearProbeTTL uint8  `json:"near_probe_ttl"`

	// FlowID
	ProbeSrcAddr   net.IP `json:"probe_src_addr"`
	ProbeDstPrefix net.IP `json:"probe_dst_prefix"`
}
