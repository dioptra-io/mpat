package queries

import (
	"errors"
	"fmt"

	v3 "github.com/dioptra-io/ufuk-research/api/v3"
)

type BasicCreateQuery struct {
	TableName           string
	AddCheckIfNotExists bool
	Database            string
	Object              any
}

func (q *BasicCreateQuery) Query() (string, error) {
	query := ""

	switch q.Object.(type) {
	case v3.IrisResultsRow:
		query = CreateIrisResultsRowQuery
	default:
		return "", errors.New("cannot find the create query for type")
	}

	notExistsCheck := ""
	if q.AddCheckIfNotExists {
		notExistsCheck = "IF NOT EXISTS"
	}

	return fmt.Sprintf(
		query,
		notExistsCheck,
		q.Database,
		q.TableName,
	), nil
}

const CreateIrisResultsRowQuery = `
CREATE TABLE %s %s.%s (
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
    rtt                    UInt16 CODEC(T64, ZSTD(1)),
    round                  UInt8,
    probe_dst_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
    reply_src_prefix       IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
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
    valid_probe_protocol   UInt8 MATERIALIZED probe_protocol IN [1, 17, 58],
    time_exceeded_reply    UInt8 MATERIALIZED (reply_protocol = 1 AND reply_icmp_type = 11) OR (reply_protocol = 58 AND reply_icmp_type = 3)
)
ENGINE MergeTree
ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl)
;`
