package arkdata

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Define a struct matching the ClickHouse table schema

type ArkClient struct {
	// Client related stuff
	BaseURL     string
	ArkUser     string
	ArkPassword string

	// Generator related stuff
	StartTime time.Time
	EndTime   time.Time
	Index     int
}

func (c *ArkClient) Length() int {
	return int((c.EndTime.Sub(c.StartTime).Hours()/24 + 1))
}

func (c *ArkClient) Reset() {
	c.Index = 0
}

func (c *ArkClient) Next() bool {
	if c.Index >= c.Length() {
		return false
	}

	c.Index += 1
	return true
}

func (c *ArkClient) CurrentDate() *time.Time {
	generatedDate := c.StartTime.Add(time.Hour * 24 * time.Duration(c.Index-1))
	return &generatedDate
}

func (c *ArkClient) CurrentDateString() string {
	currentDate := c.CurrentDate()
	return fmt.Sprintf("%d%02d%02d", currentDate.Year(), int(currentDate.Month()), int(currentDate.Day()))
}

func (c *ArkClient) CurrentCycleTableString() string {
	currentDateString := c.CurrentDateString()
	return fmt.Sprintf("cycle_%v", currentDateString)
}

func (c *ArkClient) CurrentURL() string {
	generatedDate := c.CurrentDate()
	generatedURL := fmt.Sprintf("%s/%v/cycle-%s", c.BaseURL, generatedDate.Year(), c.CurrentDateString())
	return generatedURL
}

// Downloads the wart links from the generators current cycle link
func (c *ArkClient) CurrentWartLinks() ([]string, error) {
	// Get the current URL of the cycle page.
	currentURL := c.CurrentURL()

	logger.Debugf("Sending request for list of agents: %q.\n", currentURL)

	req, err := http.NewRequest("GET", currentURL, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.ArkUser, c.ArkPassword)

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	content := string(body)

	logger.Debugf("Downloaded cycle body: %v\n", content)

	re := regexp.MustCompile("\".*gz\"")

	matches := re.FindAllString(content, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no match for the cycle-page")
	}

	urlsToDownload := make([]string, len(matches))

	for i := 0; i < len(matches); i++ {
		wartFilename := strings.ReplaceAll(matches[i], "\"", "")
		urlsToDownload[i] = fmt.Sprintf("%s/%s", currentURL, wartFilename)
	}

	return urlsToDownload, nil
}

func (p *ArkClient) DownloadRawFile(wartLink string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", wartLink, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(p.ArkUser, p.ArkPassword)

	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (p *ArkClient) DecompressRawFile(readCloser io.ReadCloser) (io.ReadCloser, error) {
	decompressedReader, err := gzip.NewReader(readCloser)
	if err != nil {
		return nil, err
	}
	return decompressedReader, nil
}

func (p *ArkClient) ConvertDecompressedFile(wartsReader io.ReadCloser) (io.ReadCloser, error) {
	pantrace := exec.Command("pantrace", "--from", "scamper-trace-warts", "--to", "iris")
	stdin, err := pantrace.StdinPipe()
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

	go func() {
		defer stdin.Close()
		if numBytesRead, err := io.Copy(stdin, wartsReader); err != nil {
			logger.Panicf("Error while converting the wart using pantrace: %v.\n", err)
		} else {
			logger.Debugf("Conversion with pantrace resulted %v bytes.\n", numBytesRead)
		}
	}()

	return stdout, nil
}

type Flow struct {
	ProbeSrcPort int             `json:"probe_src_port"`
	ProbeDstPort int             `json:"probe_dst_port"`
	Replies      [][]interface{} `json:"replies"`
}

type Traceroute struct {
	MeasurementUUID string    `json:"measurement_uuid"`
	AgentUUID       string    `json:"agent_uuid"`
	TracerouteStart time.Time `json:"traceroute_start"`
	TracerouteEnd   time.Time `json:"traceroute_end"`
	ProbeProtocol   int       `json:"probe_protocol"`
	ProbeSrcAddr    string    `json:"probe_src_addr"`
	ProbeDstAddr    string    `json:"probe_dst_addr"`
	Flows           []Flow    `json:"flows"`
}

func (p *ArkClient) ParseJSONlToResultsCSV(wartsReader io.ReadCloser) (io.ReadCloser, error) {
	lineScanner := bufio.NewScanner(wartsReader)
	for lineScanner.Scan() {
		var routeTrace Traceroute
		line := lineScanner.Bytes()
		json.Unmarshal(line, &routeTrace)

		logger.Infof("json_lines: %q.\n", routeTrace)
	}
	return nil, nil
}

func (p *ArkClient) UploadConvertedFile(conn clickhouse.Conn, wartReader io.ReadCloser, database, tableName string) error {
	p.CreateResultsTableIfNotExists(conn, database, tableName)

	insertQuery := p.InsertStatement(database, tableName)
	batch, err := conn.PrepareBatch(context.Background(), insertQuery)
	if err != nil {
		return err
	}

	lineScanner := bufio.NewScanner(wartReader)
	for lineScanner.Scan() {
		var routeTrace Traceroute
		line := lineScanner.Bytes()
		if err := json.Unmarshal(line, &routeTrace); err != nil {
			logger.Panicf("Error on json parsing: %v.\n", err)
			return err
		}

		logger.Debugf("Started uploading %d element(s).\n", len(routeTrace.Flows[0].Replies))
		// Assumed the flows has always one element.
		for _, reply := range routeTrace.Flows[0].Replies {
			captureTimestamp, err := time.Parse(time.RFC3339Nano, reply[0].(string))
			if err != nil {
				return err
			}
			probeProtocol := routeTrace.ProbeProtocol
			probeSrcAddr := net.ParseIP(routeTrace.ProbeSrcAddr)
			probeDstAddr := net.ParseIP(routeTrace.ProbeDstAddr)
			probeSrcPort := routeTrace.Flows[0].ProbeSrcPort
			probeDstPort := routeTrace.Flows[0].ProbeDstPort
			probeTTL := reply[1].(float64)
			quotedTTL := reply[2].(float64)
			replySrcAddr := net.ParseIP(reply[8].(string))
			replyProtocol := 1
			replyICMPType := 11
			replyICMPCode := 0
			replyTTL := reply[5].(float64)
			replySize := reply[6].(float64)
			// replyMplsLabels := reply[7].([]any)
			rtt := reply[9].(float64)
			round := 0

			d := ProbeData{
				CaptureTimestamp: captureTimestamp,
				ProbeProtocol:    uint8(probeProtocol),
				ProbeSrcAddr:     probeSrcAddr,
				ProbeDstAddr:     probeDstAddr,
				ProbeSrcPort:     uint16(probeSrcPort),
				ProbeDstPort:     uint16(probeDstPort),
				ProbeTTL:         uint8(probeTTL),
				QuotedTTL:        uint8(quotedTTL),
				ReplySrcAddr:     replySrcAddr,
				ReplyProtocol:    uint8(replyProtocol),
				ReplyICMPType:    uint8(replyICMPType),
				ReplyICMPCode:    uint8(replyICMPCode),
				ReplyTTL:         uint8(replyTTL),
				ReplySize:        uint16(replySize),
				// For not I am not parsing the MPLS labels.
				ReplyMPLSLabels: []struct {
					Label         uint32
					Exp           uint8
					BottomOfStack uint8
					TTL           uint8
				}{},
				RTT:   uint16(rtt),
				Round: uint8(round),
			}

			batch.Append(d.CaptureTimestamp,
				d.ProbeProtocol,
				d.ProbeSrcAddr,
				d.ProbeDstAddr,
				d.ProbeSrcPort,
				d.ProbeDstPort,
				d.ProbeTTL,
				d.QuotedTTL,
				d.ReplySrcAddr,
				d.ReplyProtocol,
				d.ReplyICMPType,
				d.ReplyICMPCode,
				d.ReplyTTL,
				d.ReplySize,
				d.ReplyMPLSLabels,
				d.RTT,
				d.Round)

		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	return nil
}

type ProbeData struct {
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

func (p *ArkClient) InsertStatement(database, tableName string) string {
	rawQuery := `
INSERT INTO %s.%s (
    capture_timestamp,
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    probe_ttl,
    quoted_ttl,
    reply_src_addr,
    reply_protocol,
    reply_icmp_type,
    reply_icmp_code,
    reply_ttl,
    reply_size,
    reply_mpls_labels,
    rtt,
    round
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return fmt.Sprintf(rawQuery, database, tableName)
}

func (p *ArkClient) CreateResultsTableIfNotExists(conn clickhouse.Conn, database, tableName string) error {
	// This is retrieved by diamond-miner souce code.
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
            -- Since we do not order by capture timestamp, this column compresses badly.
            -- To reduce its size, caracal outputs the timestamp with a one-second resolution (instead of one microsecond).
            -- This is sufficient to know if two replies were received close in time
            -- and avoid the inference of false links over many hours.
            capture_timestamp      DateTime CODEC(T64, ZSTD(1)),
            probe_protocol         UInt8,
            probe_src_addr         IPv6,
            probe_dst_addr         IPv6,
            probe_src_port         UInt16,
            probe_dst_port         UInt16,
            probe_ttl              UInt8,
            quoted_ttl             UInt8,
            reply_src_addr         IPv6,
            reply_protocol         UInt8,
            reply_icmp_type        UInt8,
            reply_icmp_code        UInt8,
            reply_ttl              UInt8,
            reply_size             UInt16,
            reply_mpls_labels      Array(Tuple(UInt32, UInt8, UInt8, UInt8)),
            -- The rtt column is the largest compressed column, we use T64 and ZSTD to reduce its size, see:
            -- https://altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse
            -- https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#codecs
            rtt                    UInt16 CODEC(T64, ZSTD(1)),
            round                  UInt8,
            -- Materialized columns
            probe_dst_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
            reply_src_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
            -- https://en.wikipedia.org/wiki/Reserved_IP_addresses
            private_probe_dst_prefix UInt8 MATERIALIZED
                (probe_dst_prefix >= toIPv6('0.0.0.0')      AND probe_dst_prefix <= toIPv6('0.255.255.255'))   OR
                (probe_dst_prefix >= toIPv6('10.0.0.0')     AND probe_dst_prefix <= toIPv6('10.255.255.255'))  OR
                (probe_dst_prefix >= toIPv6('100.64.0.0')   AND probe_dst_prefix <= toIPv6('100.127.255.255')) OR
                (probe_dst_prefix >= toIPv6('127.0.0.0')    AND probe_dst_prefix <= toIPv6('127.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('172.16.0.0')   AND probe_dst_prefix <= toIPv6('172.31.255.255'))  OR
                (probe_dst_prefix >= toIPv6('192.0.0.0')    AND probe_dst_prefix <= toIPv6('192.0.0.255'))     OR
                (probe_dst_prefix >= toIPv6('192.0.2.0')    AND probe_dst_prefix <= toIPv6('192.0.2.255'))     OR
                (probe_dst_prefix >= toIPv6('192.88.99.0')  AND probe_dst_prefix <= toIPv6('192.88.99.255'))   OR
                (probe_dst_prefix >= toIPv6('192.168.0.0')  AND probe_dst_prefix <= toIPv6('192.168.255.255')) OR
                (probe_dst_prefix >= toIPv6('198.18.0.0')   AND probe_dst_prefix <= toIPv6('198.19.255.255'))  OR
                (probe_dst_prefix >= toIPv6('198.51.100.0') AND probe_dst_prefix <= toIPv6('198.51.100.255'))  OR
                (probe_dst_prefix >= toIPv6('203.0.113.0')  AND probe_dst_prefix <= toIPv6('203.0.113.255'))   OR
                (probe_dst_prefix >= toIPv6('224.0.0.0')    AND probe_dst_prefix <= toIPv6('239.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('233.252.0.0')  AND probe_dst_prefix <= toIPv6('233.252.0.255'))   OR
                (probe_dst_prefix >= toIPv6('240.0.0.0')    AND probe_dst_prefix <= toIPv6('255.255.255.255')) OR
                (probe_dst_prefix >= toIPv6('fd00::')       AND probe_dst_prefix <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')),
            private_reply_src_addr UInt8 MATERIALIZED
                (reply_src_addr >= toIPv6('0.0.0.0')        AND reply_src_addr <= toIPv6('0.255.255.255'))     OR
                (reply_src_addr >= toIPv6('10.0.0.0')       AND reply_src_addr <= toIPv6('10.255.255.255'))    OR
                (reply_src_addr >= toIPv6('100.64.0.0')     AND reply_src_addr <= toIPv6('100.127.255.255'))   OR
                (reply_src_addr >= toIPv6('127.0.0.0')      AND reply_src_addr <= toIPv6('127.255.255.255'))   OR
                (reply_src_addr >= toIPv6('172.16.0.0')     AND reply_src_addr <= toIPv6('172.31.255.255'))    OR
                (reply_src_addr >= toIPv6('192.0.0.0')      AND reply_src_addr <= toIPv6('192.0.0.255'))       OR
                (reply_src_addr >= toIPv6('192.0.2.0')      AND reply_src_addr <= toIPv6('192.0.2.255'))       OR
                (reply_src_addr >= toIPv6('192.88.99.0')    AND reply_src_addr <= toIPv6('192.88.99.255'))     OR
                (reply_src_addr >= toIPv6('192.168.0.0')    AND reply_src_addr <= toIPv6('192.168.255.255'))   OR
                (reply_src_addr >= toIPv6('198.18.0.0')     AND reply_src_addr <= toIPv6('198.19.255.255'))    OR
                (reply_src_addr >= toIPv6('198.51.100.0')   AND reply_src_addr <= toIPv6('198.51.100.255'))    OR
                (reply_src_addr >= toIPv6('203.0.113.0')    AND reply_src_addr <= toIPv6('203.0.113.255'))     OR
                (reply_src_addr >= toIPv6('224.0.0.0')      AND reply_src_addr <= toIPv6('239.255.255.255'))   OR
                (reply_src_addr >= toIPv6('233.252.0.0')    AND reply_src_addr <= toIPv6('233.252.0.255'))     OR
                (reply_src_addr >= toIPv6('240.0.0.0')      AND reply_src_addr <= toIPv6('255.255.255.255'))   OR
                (reply_src_addr >= toIPv6('fd00::')         AND reply_src_addr <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')),
            destination_host_reply   UInt8 MATERIALIZED probe_dst_addr = reply_src_addr,
            destination_prefix_reply UInt8 MATERIALIZED probe_dst_prefix = reply_src_prefix,
            -- ICMP: protocol 1, UDP: protocol 17, ICMPv6: protocol 58
            valid_probe_protocol   UInt8 MATERIALIZED probe_protocol IN [1, 17, 58],
            time_exceeded_reply    UInt8 MATERIALIZED (reply_protocol = 1 AND reply_icmp_type = 11) OR (reply_protocol = 58 AND reply_icmp_type = 3)
        )
        ENGINE MergeTree
        ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl)
        `,
		database,
		tableName,
	)

	return conn.Exec(context.Background(), query)
}

func (p *ArkClient) GetTableSize(conn clickhouse.Conn, database, tableName string) (int, error) {
	query := fmt.Sprintf(
		"SELECT count(*) as count FROM %s.%s FORMAT CSV",
		database,
		tableName,
	)
	var count uint64
	if err := conn.QueryRow(context.Background(), query).Scan(&count); err != nil {
		return 0, err
	}

	return int(count), nil
}
