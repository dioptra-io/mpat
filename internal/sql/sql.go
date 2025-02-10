package sql

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func CheckTableExists(
	conn clickhouse.Conn,
	ctx context.Context,
	database, tableName string,
) (bool, error) {
	var exists uint8
	existsQuery := fmt.Sprintf("EXISTS %s.%s;", database, tableName)

	// Run the query
	err := conn.QueryRow(ctx, existsQuery).Scan(&exists)
	if err != nil {
		return false, err
	}

	if exists == 1 {
		return true, nil
	}
	return false, nil
}

func CreateRoutesTable(
	conn clickhouse.Conn,
	ctx context.Context,
	database, routesTableName string,
) error {
	raw := `CREATE TABLE %s.%s (
    ip_addr IPv6,
    dst_prefix IPv6,
    next_addr IPv6,
    PRIMARY KEY (ip_addr, dst_prefix, next_addr)
) ENGINE = MergeTree()
ORDER BY (ip_addr, dst_prefix, next_addr)`

	createQuery := fmt.Sprintf(raw, database, routesTableName)

	// Run the query
	err := conn.Exec(ctx, createQuery)
	if err != nil {
		return err
	}
	return nil
}

func InsertIntoRoutesFromResults(
	conn clickhouse.Conn,
	ctx context.Context,
	database, routesTableName, resultsTableName string,
) error {
	rawInsertQuery := `
    INSERT INTO %s.%s
        WITH
            toIPv6('::') AS null_ip,
            results_table as (
                SELECT
                    probe_dst_addr,
                    probe_src_addr,
                    probe_dst_port,
                    probe_src_port,
                    probe_protocol,
                    probe_ttl,
                    reply_src_addr
                FROM
                    %s.%s
                %s
            )
        SELECT
            DISTINCT ip_addr, dst_prefix, next_addr
        FROM (
            WITH
                groupUniqArray((probe_ttl, reply_src_addr)) as route_traces,
                arrayMap(x -> x.1, route_traces) as ttl_array,
                arrayMap(x -> x.2, route_traces) as address_array,
                range(toUInt8(arrayMin(ttl_array)), toUInt8(arrayMax(ttl_array) - 1)) as ttl_range,
                CAST((ttl_array, address_array), 'Map(UInt8, IPv6)') as route_traces_map,
                arrayMap(i -> (route_traces_map[toUInt8(i)], route_traces_map[toUInt8(i + 1)]), ttl_range) AS links,
                arrayFilter(x -> x.1 <> null_ip and x.2 <> null_ip, links) as filtered_links,
                arrayJoin(filtered_links) AS link
            SELECT
                probe_dst_addr,
                probe_src_addr,
                toIPv6(cutIPv6(probe_dst_addr, 0, 1)) as dst_prefix,
                link.1 as ip_addr,
                link.2 as next_addr
            FROM
                results_table
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
        )
        ORDER BY
            ip_addr,
            dst_prefix,
            next_addr`

	limitString := "LIMIT 10000"

	insertQuery := fmt.Sprintf(
		rawInsertQuery,
		database,
		routesTableName,
		database,
		resultsTableName,
		limitString,
	)

	err := conn.Exec(ctx, insertQuery)
	if err != nil {
		return err
	}
	return nil
}
