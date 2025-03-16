package query

import (
	"fmt"
	"regexp"
	"strings"
)

func SelectCount(tableName string) string {
	formatString := `
SELECT count(*) FROM %s
`
	return fmt.Sprintf(formatString, tableName)
}

func SelectLimitOffsetFormat(tableName string, limit, offset int, format string) string {
	formatString := `SELECT * FROM %s LIMIT %d OFFSET %d FORMAT %s`
	return fmt.Sprintf(formatString, tableName, limit, offset, format)
}

func InsertFormat(tableName string, format string) string {
	formatString := `INSERT INTO %s FORMAT %s`
	return fmt.Sprintf(formatString, tableName, format)
}

func DropTable(tableName string, addIfExists bool) string {
	ifExists := ""
	if addIfExists {
		ifExists = "IF EXISTS"
	}
	formatString := `
DROP TABLE %s %s
`
	return fmt.Sprintf(formatString, ifExists, tableName)
}

func CreateResultsTable(tableName string, addIfExists bool) string {
	ifExists := ""
	if addIfExists {
		ifExists = "IF NOT EXISTS"
	}

	formatString := `
CREATE TABLE %s %s (
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
`
	return fmt.Sprintf(formatString, ifExists, tableName)
}

func InsertResultsWithoutMPLSLables(tableName string) string {
	formatString := `
INSERT INTO %s (
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
    rtt, 
    round) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
	return fmt.Sprintf(formatString, tableName)
}

func SelectRoutes(database string, tableNames []string) string {
	formatString := `
SELECT
    probe_dst_addr,
    probe_src_addr,
    probe_dst_port,
    probe_src_port,
    probe_protocol,
    groupArray(probe_ttl) as probe_ttls,
    groupArray(capture_timestamp) as capture_timestamps,
    groupArray(reply_src_addr) as reply_src_addrs,
    -- These are the other data useful for later
    groupArray(destination_host_reply) as destination_host_replies,
    groupArray(destination_prefix_reply) as destination_prefix_replies,
    groupArray(reply_icmp_type) as reply_icmp_types,
    groupArray(reply_icmp_code) as reply_icmp_codes,
    groupArray(reply_size) as reply_sizes,
    groupArray(rtt) as rtts,
    groupArray(time_exceeded_reply) as time_exceeded_replies
FROM
    merge('%s', '%s')
GROUP BY
    probe_dst_addr,
    probe_src_addr,
    probe_dst_port,
    probe_src_port,
    probe_protocol
 ORDER BY
     probe_dst_addr,
     probe_src_addr,
     probe_dst_port,
     probe_src_port,
     probe_protocol
LIMIT 
    1000000
`
	escapedTableNames := make([]string, 0)
	for _, tableName := range tableNames {
		escapedTableNames = append(escapedTableNames, regexp.QuoteMeta(tableName))
	}
	return fmt.Sprintf(formatString, database, strings.Join(escapedTableNames, "|"))
}
