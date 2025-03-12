package api

import (
	"database/sql"
	"net"
	"reflect"
	"time"
)

type MPLSTuple struct {
	First  uint32 `db:"first"`  // UInt32
	Second uint8  `db:"second"` // UInt8
	Third  uint8  `db:"third"`  // UInt8
	Fourth uint8  `db:"fourth"` // UInt8
}

type ResultsTableRow struct {
	CaptureTimestamp       time.Time   `db:"capture_timestamp"`        // DateTime
	ProbeProtocol          uint8       `db:"probe_protocol"`           // UInt8
	ProbeSrcAddr           net.IP      `db:"probe_src_addr"`           // IPv6
	ProbeDstAddr           net.IP      `db:"probe_dst_addr"`           // IPv6
	ProbeSrcPort           uint16      `db:"probe_src_port"`           // UInt16
	ProbeDstPort           uint16      `db:"probe_dst_port"`           // UInt16
	ProbeTTL               uint8       `db:"probe_ttl"`                // UInt8
	QuotedTTL              uint8       `db:"quoted_ttl"`               // UInt8
	ReplySrcAddr           net.IP      `db:"reply_src_addr"`           // IPv6
	ReplyProtocol          uint8       `db:"reply_protocol"`           // UInt8
	ReplyICMPType          uint8       `db:"reply_icmp_type"`          // UInt8
	ReplyICMPCode          uint8       `db:"reply_icmp_code"`          // UInt8
	ReplyTTL               uint8       `db:"reply_ttl"`                // UInt8
	ReplySize              uint16      `db:"reply_size"`               // UInt16
	ReplyMPLSLabels        []MPLSTuple `db:"reply_mpls_labels"`        // Array(Tuple(UInt32, UInt8, UInt8, UInt8))
	RTT                    uint16      `db:"rtt"`                      // UInt16
	Round                  uint8       `db:"round"`                    // UInt8
	ProbeDstPrefix         net.IP      `db:"probe_dst_prefix"`         // IPv6 MATERIALIZED
	ReplySrcPrefix         net.IP      `db:"reply_src_prefix"`         // IPv6 MATERIALIZED
	PrivateProbeDstPrefix  uint8       `db:"private_probe_dst_prefix"` // UInt8 MATERIALIZED
	PrivateReplySrcAddr    uint8       `db:"private_reply_src_addr"`   // UInt8 MATERIALIZED
	DestinationHostReply   uint8       `db:"destination_host_reply"`   // UInt8 MATERIALIZED
	DestinationPrefixReply uint8       `db:"destination_prefix_reply"` // UInt8 MATERIALIZED
	ValidProbeProtocol     uint8       `db:"valid_probe_protocol"`     // UInt8 MATERIALIZED
	TimeExceededReply      uint8       `db:"time_exceeded_reply"`      // UInt8 MATERIALIZED
}

// Custom Scan function for TableSchema using tags
func (t *ResultsTableRow) Scan(rows *sql.Rows) error {
	// Get the columns from the query result
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Prepare a slice to hold pointers to the struct fields
	values := make([]interface{}, len(columns))

	// Use reflection to access the struct
	structVal := reflect.ValueOf(t).Elem()

	// For each column, find the corresponding struct field by tag
	for i, col := range columns {
		field := structVal.FieldByNameFunc(func(name string) bool {
			// Check if the struct field has the correct db tag
			field := structVal.FieldByName(name)
			tag := field.
			return tag == col
		})

		if field.IsValid() {
			// Create a pointer to the field's value
			fieldPointer := reflect.New(field.Type()).Interface()

			// Add the pointer to the values slice
			values[i] = fieldPointer
		}
	}

	// Scan the row into the values
	if err := rows.Scan(values...); err != nil {
		return err
	}

	// Manually handle special cases for complex fields like `reply_mpls_labels`
	for i, col := range columns {
		if col == "reply_mpls_labels" {
			// Cast the value to the expected type for this column
			if val, ok := values[i].([][]interface{}); ok {
				var mplsLabels []MPLSTuple
				for _, tuple := range val {
					if len(tuple) == 4 {
						mplsLabels = append(mplsLabels, MPLSTuple{
							First:  tuple[0].(uint32),
							Second: tuple[1].(uint8),
							Third:  tuple[2].(uint8),
							Fourth: tuple[3].(uint8),
						})
					}
				}
				t.ReplyMPLSLabels = mplsLabels
			}
		}
	}

	return nil
}
