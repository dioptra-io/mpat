package copyiristables

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// This is the config file that the database uses.
type DatabaseConfig struct {
	User      string
	Password  string
	Database  string
	Host      string
	ChunkSize int
	TableType string
}

func (c *DatabaseConfig) GetTablesForMeasurementUUID(measurementUUID string) ([]string, error) {
	sanitizedMeasurmenetUUID := strings.ReplaceAll(measurementUUID, "-", "_")
	query := fmt.Sprintf(
		"SHOW TABLES FROM %s LIKE 'results__%s__%%'",
		c.Database,
		sanitizedMeasurmenetUUID,
	)
	resp, err := c.Execute(query)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	tableNames := make([]string, 0)
	lineScanner := bufio.NewScanner(resp.Body)

	for lineScanner.Scan() {
		tableNames = append(tableNames, lineScanner.Text())
	}

	return tableNames, nil
}

func (c *DatabaseConfig) GetTableSize(tableName string) (int, error) {
	query := fmt.Sprintf(
		"SELECT count(*) FROM %s.%s",
		c.Database,
		tableName,
	)
	resp, err := c.Execute(query)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	trimmedResponse := string(data)
	for _, char := range []string{"\n", "\"", " "} {
		trimmedResponse = strings.ReplaceAll(trimmedResponse, char, "")
	}

	tableSize, err := strconv.Atoi(trimmedResponse)
	if err != nil {
		return 0, err
	}

	return tableSize, nil
}

func (c *DatabaseConfig) CreateResultsTableIfNotExists(tableName string) (bool, error) {
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
		c.Database,
		tableName,
	)

	// logger.Debugf("query: %v\n", query)

	resp, err := c.Execute(query)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	if size, err := c.GetTableSize(tableName); err != nil {
		return false, err
	} else {
		return size != 0, nil
	}
}

func (c *DatabaseConfig) DownloadTable(tableName string, chunk int) (io.ReadCloser, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s LIMIT %v OFFSET %v FORMAT %s",
		c.Database,
		tableName,
		c.ChunkSize,
		chunk,
		c.TableType,
	)
	resp, err := c.Execute(query)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (c *DatabaseConfig) UploadTable(tableName string, reader io.Reader) error {
	query := fmt.Sprintf(
		"INSERT INTO %s.%s FORMAT %s",
		c.Database,
		tableName,
		c.TableType,
	)
	resp, err := c.ExecuteWithData(query, reader)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		dd, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		errorString := string(dd)
		return fmt.Errorf("%s", errorString)
	}
	return err
}

func (c *DatabaseConfig) TruncateTable(tableName string) error {
	query := fmt.Sprintf(
		"TRUNCATE TABLE %s.%s",
		c.Database,
		tableName,
	)
	resp, err := c.Execute(query)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		dd, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		errorString := string(dd)
		return fmt.Errorf("%s", errorString)
	}
	return err
}

// Puts the query in the data section
func (c *DatabaseConfig) Execute(query string) (*http.Response, error) {
	queryBuffer := bytes.NewBuffer([]byte(query))
	req, err := http.NewRequest("POST", c.Host, queryBuffer)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept-Encoding", "gzip")
	req.SetBasicAuth(c.User, c.Password)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Puts the query in the parameters section and puts the data in the data section
// Note that his reduces the max length for the query.
func (c *DatabaseConfig) ExecuteWithData(query string, reader io.Reader) (*http.Response, error) {
	params := url.Values{}
	params.Set("query", query) // `url.Values` automatically handles escaping
	parameterizedURL := fmt.Sprintf("%s?%s", c.Host, params.Encode())
	req, err := http.NewRequest("POST", parameterizedURL, reader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept-Encoding", "gzip")

	req.SetBasicAuth(c.User, c.Password)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
