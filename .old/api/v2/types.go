package v2

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"time"
)

// MPLSLabel represents a tuple of MPLS label fields
type IrisMPLSLabel struct {
	Label         uint32
	Exp           uint8
	BottomOfStack uint8
	TTL           uint8
}

// IrisResultsRow represents a single entry in the results table.
type IrisResultsRow struct {
	CaptureTimestamp       time.Time       `json:"capture_timestamp"`
	ProbeProtocol          uint8           `json:"probe_protocol"`
	ProbeSrcAddr           net.IP          `json:"probe_src_addr"`
	ProbeDstAddr           net.IP          `json:"probe_dst_addr"`
	ProbeSrcPort           uint16          `json:"probe_src_port"`
	ProbeDstPort           uint16          `json:"probe_dst_port"`
	ProbeTTL               uint8           `json:"probe_ttl"`
	QuotedTTL              uint8           `json:"quoted_ttl"`
	ReplySrcAddr           net.IP          `json:"reply_src_addr"`
	ReplyProtocol          uint8           `json:"reply_protocol"`
	ReplyICMPType          uint8           `json:"reply_icmp_type"`
	ReplyICMPCode          uint8           `json:"reply_icmp_code"`
	ReplyTTL               uint8           `json:"reply_ttl"`
	ReplySize              uint16          `json:"reply_size"`
	ReplyMPLSLabels        []IrisMPLSLabel `json:"reply_mpls_labels"`
	RTT                    uint16          `json:"rtt"`
	Round                  uint8           `json:"round"`
	ProbeDstPrefix         net.IP          `json:"probe_dst_prefix"`         // Computed
	ReplySrcPrefix         net.IP          `json:"reply_src_prefix"`         // Computed
	PrivateProbeDstPrefix  uint8           `json:"private_probe_dst_prefix"` // Computed
	PrivateReplySrcAddr    uint8           `json:"private_reply_src_addr"`   // Computed
	DestinationHostReply   uint8           `json:"destination_host_reply"`   // Computed
	DestinationPrefixReply uint8           `json:"destination_prefix_reply"` // Computed
	ValidProbeProtocol     uint8           `json:"valid_probe_protocol"`     // Computed
	TimeExceededReply      uint8           `json:"time_exceeded_reply"`      // Computed
}

func (r *IrisResultsRow) Json() string {
	// Define a helper struct for custom marshaling
	type Alias struct {
		CaptureTimestamp       time.Time       `json:"capture_timestamp"`
		ProbeProtocol          []int           `json:"probe_protocol"`
		ProbeSrcAddr           string          `json:"probe_src_addr"`
		ProbeDstAddr           string          `json:"probe_dst_addr"`
		ProbeSrcPort           []int           `json:"probe_src_port"`
		ProbeDstPort           []int           `json:"probe_dst_port"`
		ProbeTTL               []int           `json:"probe_ttl"`
		QuotedTTL              []int           `json:"quoted_ttl"`
		ReplySrcAddr           string          `json:"reply_src_addr"`
		ReplyProtocol          []int           `json:"reply_protocol"`
		ReplyICMPType          []int           `json:"reply_icmp_type"`
		ReplyICMPCode          []int           `json:"reply_icmp_code"`
		ReplyTTL               []int           `json:"reply_ttl"`
		ReplySize              []int           `json:"reply_size"`
		ReplyMPLSLabels        []IrisMPLSLabel `json:"reply_mpls_labels"`
		RTT                    []int           `json:"rtt"`
		Round                  []int           `json:"round"`
		ProbeDstPrefix         string          `json:"probe_dst_prefix"`
		ReplySrcPrefix         string          `json:"reply_src_prefix"`
		PrivateProbeDstPrefix  []int           `json:"private_probe_dst_prefix"`
		PrivateReplySrcAddr    []int           `json:"private_reply_src_addr"`
		DestinationHostReply   []int           `json:"destination_host_reply"`
		DestinationPrefixReply []int           `json:"destination_prefix_reply"`
		ValidProbeProtocol     []int           `json:"valid_probe_protocol"`
		TimeExceededReply      []int           `json:"time_exceeded_reply"`
	}

	alias := Alias{
		CaptureTimestamp:       r.CaptureTimestamp,
		ProbeProtocol:          []int{int(r.ProbeProtocol)},
		ProbeSrcAddr:           r.ProbeSrcAddr.String(),
		ProbeDstAddr:           r.ProbeDstAddr.String(),
		ProbeSrcPort:           []int{int(r.ProbeSrcPort)},
		ProbeDstPort:           []int{int(r.ProbeDstPort)},
		ProbeTTL:               []int{int(r.ProbeTTL)},
		QuotedTTL:              []int{int(r.QuotedTTL)},
		ReplySrcAddr:           r.ReplySrcAddr.String(),
		ReplyProtocol:          []int{int(r.ReplyProtocol)},
		ReplyICMPType:          []int{int(r.ReplyICMPType)},
		ReplyICMPCode:          []int{int(r.ReplyICMPCode)},
		ReplyTTL:               []int{int(r.ReplyTTL)},
		ReplySize:              []int{int(r.ReplySize)},
		ReplyMPLSLabels:        r.ReplyMPLSLabels,
		RTT:                    []int{int(r.RTT)},
		Round:                  []int{int(r.Round)},
		ProbeDstPrefix:         r.ProbeDstPrefix.String(),
		ReplySrcPrefix:         r.ReplySrcPrefix.String(),
		PrivateProbeDstPrefix:  []int{int(r.PrivateProbeDstPrefix)},
		PrivateReplySrcAddr:    []int{int(r.PrivateReplySrcAddr)},
		DestinationHostReply:   []int{int(r.DestinationHostReply)},
		DestinationPrefixReply: []int{int(r.DestinationPrefixReply)},
		ValidProbeProtocol:     []int{int(r.ValidProbeProtocol)},
		TimeExceededReply:      []int{int(r.TimeExceededReply)},
	}

	data, err := json.Marshal(alias)
	if err != nil {
		return "{}" // Or handle error as needed
	}
	return string(data)
}

func ScanIrisResultsRow(row *sql.Rows) (*IrisResultsRow, error) {
	var r IrisResultsRow
	var replyMPLSLabelsRaw [][]interface{} // To hold Array(Tuple(...))

	err := row.Scan(
		&r.CaptureTimestamp,
		&r.ProbeProtocol,
		&r.ProbeSrcAddr,
		&r.ProbeDstAddr,
		&r.ProbeSrcPort,
		&r.ProbeDstPort,
		&r.ProbeTTL,
		&r.QuotedTTL,
		&r.ReplySrcAddr,
		&r.ReplyProtocol,
		&r.ReplyICMPType,
		&r.ReplyICMPCode,
		&r.ReplyTTL,
		&r.ReplySize,
		&replyMPLSLabelsRaw,
		&r.RTT,
		&r.Round,
		&r.ProbeDstPrefix,
		&r.ReplySrcPrefix,
		&r.PrivateProbeDstPrefix,
		&r.PrivateReplySrcAddr,
		&r.DestinationHostReply,
		&r.DestinationPrefixReply,
		&r.ValidProbeProtocol,
		&r.TimeExceededReply,
	)
	if err != nil {
		return nil, err
	}

	// Parse MPLS labels
	for _, tuple := range replyMPLSLabelsRaw {
		if len(tuple) != 4 {
			return nil, errors.New("invalid MPLS label tuple length")
		}

		label, ok1 := tuple[0].(uint32)
		exp, ok2 := tuple[1].(uint8)
		bos, ok3 := tuple[2].(uint8)
		ttl, ok4 := tuple[3].(uint8)

		if !ok1 || !ok2 || !ok3 || !ok4 {
			return nil, errors.New("invalid MPLS label tuple type")
		}

		r.ReplyMPLSLabels = append(r.ReplyMPLSLabels, IrisMPLSLabel{
			Label:         label,
			Exp:           exp,
			BottomOfStack: bos,
			TTL:           ttl,
		})
	}

	return &r, nil
}

// IrisRouteTrace represents a single routetrace where all the entries are groupped by the flowid.
type IrisRouteTrace struct {
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

func (r *IrisRouteTrace) Json() string {
	type Alias struct {
		ProbeDstAddr             string      `json:"probe_dst_addr"`
		ProbeSrcAddr             string      `json:"probe_src_addr"`
		ProbeDstPort             int         `json:"probe_dst_port"`
		ProbeSrcPort             int         `json:"probe_src_port"`
		ProbeProtocol            int         `json:"probe_protocol"`
		ProbeTTLs                []int       `json:"probe_ttls"`
		CaptureTimestamps        []time.Time `json:"capture_timestamps"`
		ReplySrcAddrs            []string    `json:"reply_src_addrs"`
		DestinationHostReplies   []int       `json:"destination_host_replies"`
		DestinationPrefixReplies []int       `json:"destination_prefix_replies"`
		ReplyICMPTypes           []int       `json:"reply_icmp_types"`
		ReplyICMPCodes           []int       `json:"reply_icmp_codes"`
		ReplySizes               []int       `json:"reply_sizes"`
		RTTs                     []int       `json:"rtts"`
		TimeExceededReplies      []int       `json:"time_exceeded_replies"`
	}

	// Convert []uint8 and []uint16 to []int, and net.IP to string
	alias := Alias{
		ProbeDstAddr:             r.ProbeDstAddr.String(),
		ProbeSrcAddr:             r.ProbeSrcAddr.String(),
		ProbeDstPort:             int(r.ProbeDstPort),
		ProbeSrcPort:             int(r.ProbeSrcPort),
		ProbeProtocol:            int(r.ProbeProtocol),
		ProbeTTLs:                uint8SliceToIntSlice(r.ProbeTTLs),
		CaptureTimestamps:        r.CaptureTimestamps,
		ReplySrcAddrs:            ipSliceToStringSlice(r.ReplySrcAddrs),
		DestinationHostReplies:   uint8SliceToIntSlice(r.DestinationHostReplies),
		DestinationPrefixReplies: uint8SliceToIntSlice(r.DestinationPrefixReplies),
		ReplyICMPTypes:           uint8SliceToIntSlice(r.ReplyICMPTypes),
		ReplyICMPCodes:           uint8SliceToIntSlice(r.ReplyICMPCodes),
		ReplySizes:               uint16SliceToIntSlice(r.ReplySizes),
		RTTs:                     uint16SliceToIntSlice(r.RTTs),
		TimeExceededReplies:      uint8SliceToIntSlice(r.TimeExceededReplies),
	}

	data, err := json.Marshal(alias)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// Helper: Convert []uint8 to []int
func uint8SliceToIntSlice(s []uint8) []int {
	res := make([]int, len(s))
	for i, v := range s {
		res[i] = int(v)
	}
	return res
}

// Helper: Convert []uint16 to []int
func uint16SliceToIntSlice(s []uint16) []int {
	res := make([]int, len(s))
	for i, v := range s {
		res[i] = int(v)
	}
	return res
}

// Helper: Convert []net.IP to []string
func ipSliceToStringSlice(s []net.IP) []string {
	res := make([]string, len(s))
	for i, ip := range s {
		res[i] = ip.String()
	}
	return res
}

// This method scans and creates a ScanIrisRouteTrace
func ScanIrisRouteTrace(row *sql.Rows) (*IrisRouteTrace, error) {
	var r IrisRouteTrace

	err := row.Scan(
		&r.ProbeProtocol,
		&r.ProbeSrcAddr,
		&r.ProbeDstAddr,
		&r.ProbeSrcPort,
		&r.ProbeDstPort,
		&r.ProbeTTLs,                // groupArray of probe_ttl
		&r.CaptureTimestamps,        // groupArray of capture_timestamp
		&r.ReplySrcAddrs,            // groupArray of reply_src_addr as []string
		&r.DestinationHostReplies,   // groupArray of destination_host_reply
		&r.DestinationPrefixReplies, // groupArray of destination_prefix_reply
		&r.ReplyICMPTypes,           // groupArray of reply_icmp_type
		&r.ReplyICMPCodes,           // groupArray of reply_icmp_code
		&r.ReplySizes,               // groupArray of reply_size
		&r.RTTs,                     // groupArray of rtt
		&r.TimeExceededReplies,      // groupArray of time_exceeded_reply
	)
	if err != nil {
		return nil, err
	}

	return &r, nil
}
