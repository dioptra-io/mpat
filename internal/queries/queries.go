package queries

import (
	"fmt"
	"strings"
)

func SelectFromTables(database string, tableNames []string) string {
	joinedString := strings.Join(tableNames, "|")
	return fmt.Sprintf(`
SELECT 
    * 
FROM 
    merge('%s', '%s');`, database, joinedString)
}

func SelectFromTablesGroupByFlowID(database string, tableNames []string) string {
	joinedString := strings.Join(tableNames, "|")
	return fmt.Sprintf(`
SELECT
    probe_protocol,
    probe_src_addr,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    groupArray(probe_ttl) AS probe_ttls,
    groupArray(capture_timestamp) AS capture_timestamps,
    groupArray(reply_src_addr) AS reply_src_addrs,
    groupArray(destination_host_reply) AS destination_host_replies,
    groupArray(destination_prefix_reply) AS destination_prefix_replies,
    groupArray(reply_icmp_type) AS reply_icmp_types,
    groupArray(reply_icmp_code) AS reply_icmp_codes,
    groupArray(reply_size) AS reply_sizes,
    groupArray(rtt) AS rtts,
    groupArray(time_exceeded_reply) AS time_exceeded_replies
FROM 
    merge('%s', '%s')
GROUP BY
    probe_protocol,
    probe_src_addr,
    probe_dst_prefix,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port;`, database, joinedString)
}
