package v1

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"time"

	"dioptra-io/ufuk-research/pkg/adapter"
	apiv1 "dioptra-io/ufuk-research/pkg/api/v1"
)

// This is used to convert the putput of the pantrace jsonl format to Iris native format.
// Instead of a regular conversion it returns a channel of objects.
type PantraceJSONLToProbeDataConverter struct {
	bufferSize int
}

func NewPantraceToProbeRecordConverter(bufferSize int, skipMPLSLabels bool) adapter.ConverterChan[apiv1.ProbeRecord] {
	if !skipMPLSLabels {
		panic("not yet implemented to convert the MPLS tables from the pantrace jsonl format!")
	}
	return &PantraceJSONLToProbeDataConverter{
		bufferSize: bufferSize,
	}
}

// This is some heavy lifting, converts the jsonl we read from the reader to Iris native
// CSV format just like in the results table.
func (p PantraceJSONLToProbeDataConverter) Convert(r io.Reader) (<-chan apiv1.ProbeRecord, <-chan error) {
	// This is copied from the pantrace. This is just a helper struct for conversion.
	type pantraceFlow struct {
		ProbeSrcPort uint16          `json:"probe_src_port"`
		ProbeDstPort uint16          `json:"probe_dst_port"`
		Replies      [][]interface{} `json:"replies"`
	}

	// This is copied from the pantrace. This is just a helper struct for conversion.
	type pantraceTraceroute struct {
		MeasurementUUID string         `json:"measurement_uuid"`
		AgentUUID       string         `json:"agent_uuid"`
		TracerouteStart time.Time      `json:"traceroute_start"`
		TracerouteEnd   time.Time      `json:"traceroute_end"`
		ProbeProtocol   uint8          `json:"probe_protocol"`
		ProbeSrcAddr    string         `json:"probe_src_addr"`
		ProbeDstAddr    string         `json:"probe_dst_addr"`
		Flows           []pantraceFlow `json:"flows"`
	}

	// Create the channel with the size specified
	recordCh := make(chan apiv1.ProbeRecord, p.bufferSize)
	errCh := make(chan error) // make an unbuffered channel?

	go func() {
		defer close(recordCh)
		defer close(errCh)

		// Here do the work. Read from the r io.Reader and output ProbeRecords
		lineScanner := bufio.NewScanner(r)
		for lineScanner.Scan() {
			if err := lineScanner.Err(); err != nil {
				return
			}

			var routeTrace pantraceTraceroute
			lineData := lineScanner.Bytes()

			if err := json.Unmarshal(lineData, &routeTrace); err != nil {
				errCh <- err
				return
			}

			// This part is litte compilcated since there needs to be a manual conversion.
			for _, reply := range routeTrace.Flows[0].Replies {
				captureTimestamp, err := time.Parse(time.RFC3339Nano, reply[0].(string))
				if err != nil {
					errCh <- err
					return
				}

				// Here the interfaced ones are converted to float64 immediately. So we convert them
				// back to uint8 uint16 etc.
				record := apiv1.ProbeRecord{
					CaptureTimestamp: captureTimestamp,
					ProbeProtocol:    routeTrace.ProbeProtocol,
					ProbeSrcAddr:     net.ParseIP(routeTrace.ProbeSrcAddr),
					ProbeDstAddr:     net.ParseIP(routeTrace.ProbeDstAddr),
					ProbeSrcPort:     routeTrace.Flows[0].ProbeSrcPort,
					ProbeDstPort:     routeTrace.Flows[0].ProbeDstPort,
					ProbeTTL:         uint8(reply[1].(float64)),
					QuotedTTL:        uint8(reply[2].(float64)),
					ReplySrcAddr:     net.ParseIP(reply[8].(string)),

					// Some of these values are not included and needs to be hardcoded.
					ReplyProtocol: uint8(1),
					ReplyICMPType: uint8(11),
					ReplyICMPCode: uint8(0),

					ReplyTTL:  uint8(reply[5].(float64)),
					ReplySize: uint16(reply[6].(float64)),

					// For not I am not parsing the MPLS labels.
					ReplyMPLSLabels: []struct {
						Label         uint32
						Exp           uint8
						BottomOfStack uint8
						TTL           uint8
					}{},

					// The remeaning ones goes here.
					RTT:   uint16(reply[9].(float64)),
					Round: uint8(0), // There is not round concept in Ark thus 0

				}

				// Add this to the record channel. If it is full then this routine waits.
				recordCh <- record
			}

		}
	}()

	return recordCh, errCh
}
