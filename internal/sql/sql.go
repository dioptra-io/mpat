package sql

import (
	"context"
	"fmt"
	"net"

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

func TruncateRoutesTable(
	conn clickhouse.Conn,
	ctx context.Context,
	database, routesTableName string,
) error {
	raw := `TRUNCATE TABLE %s.%s`

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
                arrayFilter(x -> x.1 <> null_ip and x.2 <> null_ip and x.1 <> dst_prefix and x.2 <> dst_prefix, links) as filtered_links,
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

	limitString := "LIMIT 0"

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

func GetRouteScoresOfAddresses(
	conn clickhouse.Conn,
	ctx context.Context,
	database, routesTableName string,
) error {
	raw := `WITH
    length(groupUniqArray(dst_prefix)) as route_score
SELECT
    ip_addr,
    route_score
FROM
    -- merge('iris', 'routes__b56098aa_896c_437c_9a98_a2c6b51a5a84__*')
    %s.%s
GROUP BY
    ip_addr
ORDER BY
    route_score DESC,
    ip_addr DESC`
	selectQuery := fmt.Sprintf(raw, database, routesTableName)

	// Run the query
	rows, err := conn.Query(ctx, selectQuery)
	if err != nil {
		return err
	}

	var address net.IP
	var routeScore uint64

	for rows.Next() {
		if err := rows.Scan(&address, &routeScore); err == nil {
			fmt.Printf("%q %v\n", address, routeScore)
		} else {
			return err
		}
	}
	return nil
}

func GetRouteScoresOfAddressesMerged(
	conn clickhouse.Conn,
	ctx context.Context,
	database, measurementUUID string,
) error {
	raw := `WITH
    length(groupUniqArray(dst_prefix)) as route_score
SELECT
    ip_addr,
    route_score
FROM
    merge('%s', 'routes__%s__*')
GROUP BY
    ip_addr
ORDER BY
    route_score DESC,
    ip_addr DESC`
	selectQuery := fmt.Sprintf(raw, database, measurementUUID)

	// Run the query
	rows, err := conn.Query(ctx, selectQuery)
	if err != nil {
		return err
	}

	var address net.IP
	var routeScore uint64

	for rows.Next() {
		if err := rows.Scan(&address, &routeScore); err == nil {
			fmt.Printf("%q %v\n", address, routeScore)
		} else {
			return err
		}
	}
	return nil
}
