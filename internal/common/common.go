package common

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/log"
)

var logger = log.GetLogger()

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

func GetExistingAndNonExistingTables(
	conn clickhouse.Conn,
	database string,
	measUUID string,
	agentUUIDs []string,
) ([]string, []string, error) {
	measUUIDFormatted := strings.ReplaceAll(measUUID, "-", "_")
	// Query to fetch existing tables
	showTablesQuery := fmt.Sprintf(
		"SHOW TABLES FROM %s LIKE 'routes__%s__%%'",
		database,
		measUUIDFormatted,
	)

	logger.Debugf("Show tables query: %v\n", showTablesQuery)
	// Execute query to get all tables from the database
	rows, err := conn.Query(context.TODO(), showTablesQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing SHOW TABLES query: %w", err)
	}
	defer rows.Close()

	// Get all of the table names.
	var databaseTableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, nil, fmt.Errorf("error scanning table name: %w", err)
		}
		databaseTableNames = append(databaseTableNames, tableName)
	}

	logger.Debugf("Scanned %v database route tables.\n", len(databaseTableNames))
	logger.Debugf("Database route tables %v.\n", databaseTableNames)

	// Generate the tables by the provided agent uuids.
	generatedTableNames := make([]string, 0)
	// If the agent UUIDs are not given then use results tables to get the agent-uuids.
	if len(agentUUIDs) != 0 {
		for _, agentUUID := range agentUUIDs {
			agentUUIDFormatted := strings.ReplaceAll(agentUUID, "-", "_")
			generatedTableName := fmt.Sprintf(
				"routes__%s__%s",
				measUUIDFormatted,
				agentUUIDFormatted,
			)
			generatedTableNames = append(generatedTableNames, generatedTableName)
		}
	} else {
		showTablesQuery := fmt.Sprintf(
			"SHOW TABLES FROM %s LIKE 'results__%s__%%'",
			database,
			measUUIDFormatted,
		)

		// Execute query to get all tables from the database
		rows, err := conn.Query(context.TODO(), showTablesQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("error executing SHOW TABLES query: %w", err)
		}
		defer rows.Close()

		// Iterate the results tables
		for rows.Next() {
			var resultsTableName string
			if err := rows.Scan(&resultsTableName); err != nil {
				return nil, nil, fmt.Errorf("error scanning table name: %w", err)
			}
			tableName := strings.Replace(resultsTableName, "results__", "routes__", 1)
			generatedTableNames = append(generatedTableNames, tableName)
		}
	}

	logger.Debugf("Generated %v route tables.\n", len(generatedTableNames))
	logger.Debugf("Route tables %v.\n", generatedTableNames)

	// generated set difference database gives non-exising tables.
	nonExistingTableNames := SetDifference(generatedTableNames, databaseTableNames)

	// generated set intersection database gives the existing tables.
	existingTableNames := SetIntersection(generatedTableNames, databaseTableNames)

	return existingTableNames, nonExistingTableNames, nil
}

// SetDifference returns elements in A that are not in B.
func SetDifference(A, B []string) []string {
	// Create a set for B
	bSet := make(map[string]struct{}, len(B))
	for _, b := range B {
		bSet[b] = struct{}{}
	}

	// Find elements in A that are not in B
	var diff []string
	for _, a := range A {
		if _, found := bSet[a]; !found {
			diff = append(diff, a)
		}
	}

	return diff
}

// SetIntersection returns the intersection of two string slices A and B.
func SetIntersection(A, B []string) []string {
	// Create a set for A
	aSet := make(map[string]struct{}, len(A))
	for _, a := range A {
		aSet[a] = struct{}{}
	}

	// Find common elements in B
	var intersection []string
	for _, b := range B {
		if _, found := aSet[b]; found {
			intersection = append(intersection, b)
		}
	}

	return intersection
}

func CreateRouteTablesIfNotExists(
	conn clickhouse.Conn,
	database string,
	tablesToCreate []string,
) error {
	for _, tableName := range tablesToCreate {

		raw := `CREATE TABLE IF NOT EXISTS %s.%s (
    ip_addr IPv6,
    dst_prefix IPv6,
    next_addr IPv6,
    PRIMARY KEY (ip_addr, dst_prefix, next_addr)
) ENGINE = MergeTree()
ORDER BY (ip_addr, dst_prefix, next_addr)`

		createQuery := fmt.Sprintf(raw, database, tableName)

		logger.Debugf("Create the routes table SQL query: %v\n", createQuery)
		// Run the query
		err := conn.Exec(context.TODO(), createQuery)
		if err != nil {
			return err
		}
		logger.Debugf("Created table '%s'.\n", tableName)

	}

	return nil
}

func TruncateTables(
	conn clickhouse.Conn,
	database string,
	tablesToTruncate []string,
) error {
	for _, tableName := range tablesToTruncate {

		raw := `TRUNCATE TABLE %s.%s`

		truncateQuery := fmt.Sprintf(raw, database, tableName)

		logger.Debugf("Truncate the routes SQL query: %v\n", truncateQuery)

		// Run the query
		err := conn.Exec(context.TODO(), truncateQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

func ComputeRouteTables(
	conn clickhouse.Conn,
	database string,
	tablesToTruncate []string,
) error {
	logger.Infoln("Started computing the routes tables.")
	for _, tableName := range tablesToTruncate {
		resultsTableName := strings.Replace(tableName, "routes__", "results__", 1)

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

		// limitString := "LIMIT 10"
		limitString := ""

		insertQuery := fmt.Sprintf(
			rawInsertQuery,
			database,
			tableName,
			database,
			resultsTableName,
			limitString,
		)

		logger.Debugf("Insert to routes SQL query: %v\n", insertQuery)

		err := conn.Exec(context.TODO(), insertQuery)
		if err != nil {
			return err
		}
		logger.Infof("Computation done for %v.\n", tableName)
	}
	logger.Infoln("Finished computing the routes tables.")
	return nil
}

func GetScoresFromRouteTables(
	conn clickhouse.Conn,
	database string,
	tableNames []string,
	addresses []string,
	outputWriter io.Writer,
) error {
	selectStatements := make([]string, 0)
	for _, tableName := range tableNames {
		selectStatements = append(
			selectStatements,
			fmt.Sprintf("SELECT * FROM %s.%s", database, tableName),
		)
	}
	unionStatement := strings.Join(selectStatements, " UNION ALL ")

	var addressConditions string
	if len(addresses) > 0 {
		quotedAddresses := make([]string, len(addresses))
		for i, addr := range addresses {
			quotedAddresses[i] = fmt.Sprintf("'%s'", addr) // Wrap IPs in single quotes
		}
		addressConditions = fmt.Sprintf(
			"WHERE ip_addr IN (%s)",
			strings.Join(quotedAddresses, ", "),
		)
	}

	raw := `WITH
    length(groupUniqArray(dst_prefix)) as route_score
SELECT
    ip_addr,
    route_score
FROM
    (%s)
%s
GROUP BY
    ip_addr
ORDER BY
    route_score DESC,
    ip_addr DESC`
	selectQuery := fmt.Sprintf(raw, unionStatement, addressConditions)

	logger.Debugf("Select SQL query: %v\n", selectQuery)
	logger.Infoln("Started writing the scores to the output file.")
	// Run the query
	rows, err := conn.Query(context.TODO(), selectQuery)
	if err != nil {
		return err
	}

	var address net.IP
	var routeScore uint64

	header := []byte("ip_addr,score\n")
	outputWriter.Write(header)
	for rows.Next() {
		if err := rows.Scan(&address, &routeScore); err == nil {
			row := []byte(fmt.Sprintf("%q, %v\n", address, routeScore))
			outputWriter.Write(row)
		} else {
			return err
		}
	}
	logger.Infoln("Finished writing the scores to the output file.")
	return nil
}
