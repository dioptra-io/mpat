package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"
)

func NewConnection() (clickhouse.Conn, error) {
	database := viper.GetString("database")
	user := viper.GetString("user")
	password := viper.GetString("password")
	host := viper.GetString("host")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		ConnMaxLifetime: time.Hour * 24 * 365,
	})

	return conn, err
}

func TableExists(conn clickhouse.Conn, ctx context.Context, tableName string) (bool, error) {
	var exists int
	existsQuery := fmt.Sprintf("EXISTS %s;", tableName)

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

func CreateTable(conn clickhouse.Conn, ctx context.Context, createQuery string) error {
	// Run the query
	err := conn.Exec(ctx, createQuery)
	if err != nil {
		return err
	}
	return nil
}

func ResultsToRoutesTableName(resultsTableName string) string {
	stem := strings.TrimPrefix(resultsTableName, "results")

	return fmt.Sprintf("routes%s", stem)
}

func SQLSelectRoutesFromResults(database, resultsTableName string) string {
	raw := `WITH
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
    )
SELECT 
    DISTINCT ip_addr, dst_prefix, next_addr
FROM (
    WITH 
    -- Creates the range for the TTL
        groupUniqArray((probe_ttl, reply_src_addr)) as route_traces,
    -- Create the ttl_array and values_array
        arrayMap(x -> x.1, route_traces) as ttl_array,
        arrayMap(x -> x.2, route_traces) as address_array,
    -- Creates the range for the TTL
        range(toUInt8(arrayMin(ttl_array)), toUInt8(arrayMax(ttl_array) - 1)) as ttl_range,
    -- Convert the route traces to a map
        CAST((ttl_array, address_array), 'Map(UInt8, IPv6)') as route_traces_map,
    -- Create the links like in the links table calculation
        arrayMap(i -> (route_traces_map[toUInt8(i)], route_traces_map[toUInt8(i + 1)]), ttl_range) AS links,
    -- Filter out the null addresses
        arrayFilter(x -> x.1 <> null_ip and x.2 <> null_ip, links) as filtered_links,
    -- Join the links
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

	return fmt.Sprintf(raw, database, resultsTableName)
}

func SQLInsertIntoRoutes(database, resultsTableName string) string {
	raw := `INSERT INTO %s.%s (%s)`
	routesTableName := ResultsToRoutesTableName(resultsTableName)
	selectRoutesQuery := SQLSelectRoutesFromResults(database, resultsTableName)
	return fmt.Sprintf(raw, database, routesTableName, selectRoutesQuery)
}
