package client

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"net"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

// This basically converts the given warts file to a pantrace iris format. It is a jsonl
// format, slightly different than the results table format. Take a look at pantrace on
// Github.
type PantraceConverter struct {
	// The output of Convert functuon needs to be closed.
	ConvertCloser

	// name or the path of the pantrace executable.
	exec       string
	fromFormat string
	toFormat   string
	logger     *logrus.Logger
}

func NewPantraceConverter(logger *logrus.Logger) *PantraceConverter {
	// 	pantrace := exec.Command("pantrace", "--from", "scamper-trace-warts", "--to", "iris")
	return &PantraceConverter{
		exec:       "pantrace",
		fromFormat: "scamper-trace-warts",
		toFormat:   "iris",
		logger:     logger,
	}
}

// Note that the actual copying is done on a separate go routine. This means that if there
// an error on the conversion we won't be able to see the actual error. Instead we would
// only see the log, and the actual error will only be realized when trying to read from
// this function's output.
//
// To overcome this we might return an error chan instead of just an error. But I am too
// lazy to implement that rn.
func (p PantraceConverter) Convert(r io.Reader) (io.ReadCloser, error) {
	pantrace := exec.Command(
		p.exec,
		"--from",
		p.fromFormat,
		"--to",
		p.toFormat)

	stdin, err := pantrace.StdinPipe()
	if err != nil {
		return nil, err
	}

	stderr, err := pantrace.StderrPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := pantrace.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := pantrace.Start(); err != nil {
		return nil, err
	}

	// This is for piping the given reader as an argument to the stdin of the process.
	// A better method might be the usage of pipes, but for now this is works.
	go func() {
		defer stdin.Close()
		if numBytesRead, err := io.Copy(stdin, r); err != nil {
			p.logger.Panicf("Error while converting the wart using pantrace: %v.\n", err)
		} else {
			p.logger.Debugf("Conversion with pantrace resulted %v bytes.\n", numBytesRead)
		}
	}()

	// This is for displaying the error message pantrace gives out
	go func() {
		defer stderr.Close()
		if data, err := io.ReadAll(stderr); err != nil {
			p.logger.Panicf("Error while reading the std err of pantrace comamnd: %v.\n", err)
		} else if len(data) != 0 {
			p.logger.Panicf("Error while converting the pantrace: %v.\n", string(data))
		}
	}()

	return stdout, nil
}

// This is used to convert the putput of the pantrace jsonl format to Iris native format.
// Instead of a regular conversion it returns a channel of objects.
type PantraceJSONLToProbeDataConverter struct {
	bufferSize int
}

// This represents one row for the Iris results table entry.
type ProbeRecord struct {
	CaptureTimestamp time.Time
	ProbeProtocol    uint8
	ProbeSrcAddr     net.IP
	ProbeDstAddr     net.IP
	ProbeSrcPort     uint16
	ProbeDstPort     uint16
	ProbeTTL         uint8
	QuotedTTL        uint8
	ReplySrcAddr     net.IP
	ReplyProtocol    uint8
	ReplyICMPType    uint8
	ReplyICMPCode    uint8
	ReplyTTL         uint8
	ReplySize        uint16
	ReplyMPLSLabels  []struct {
		Label         uint32
		Exp           uint8
		BottomOfStack uint8
		TTL           uint8
	}
	RTT   uint16
	Round uint8
}

func NewPantraceToProbeRecordConverter(bufferSize int, skipMPLSLabels bool) ConverterChan[ProbeRecord] {
	if !skipMPLSLabels {
		panic("not yet implemented to convert the MPLS tables from the pantrace jsonl format!")
	}
	return &PantraceJSONLToProbeDataConverter{
		bufferSize: bufferSize,
	}
}

// This is some heavy lifting, converts the jsonl we read from the reader to Iris native
// CSV format just like in the results table.
//

func (p PantraceJSONLToProbeDataConverter) Convert(r io.Reader) (<-chan ProbeRecord, <-chan error) {
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
	recordCh := make(chan ProbeRecord, p.bufferSize)
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
				record := ProbeRecord{
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

// very simple GZip decompressor.
type GZipConverter struct {
	ConvertCloser
}

func NewGZipConverter() *GZipConverter {
	return &GZipConverter{}
}

func (p GZipConverter) Convert(r io.Reader) (io.ReadCloser, error) {
	decompressor, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return decompressor, nil
}
