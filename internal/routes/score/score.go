package score

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal"
	"dioptra-io/ufuk-research/internal/sql"
)

var fUseMerge bool

var RoutesScoreCmd = &cobra.Command{
	Use:   "score",
	Short: "Compute the route score of the ip addresses from the routes table.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := internal.NewConnection()
		if err != nil {
			fmt.Printf("cannot establish connection with Clickhouse %s\n", err)
			return
		}

		// Get the name of the database from the config
		database := viper.GetString("database")

		if err := runScores(conn, database, args[0], fUseMerge); err != nil {
			fmt.Print(err)
			return
		}
	},
}

func init() {
	RoutesScoreCmd.PersistentFlags().
		BoolVar(&fUseMerge, "use-merge", false, "set this flag to true if the program are given meas uuid instead of the table name.")
}

func runScores(
	conn clickhouse.Conn,
	database string,
	tableOrMeasUUID string,
	useMerge bool,
) error {
	ctx := context.TODO()
	// Sanitize the given input by replacing all the '-' with '_'.
	sanitizedTableOrMeasUUID := strings.ReplaceAll(tableOrMeasUUID, "-", "_")
	// No SQL computation of the routes table is not yet supported.
	if useMerge {
		if err := sql.GetRouteScoresOfAddressesMerged(conn, ctx, database, sanitizedTableOrMeasUUID); err != nil {
			return err
		}
	} else {
		routesTableName := fmt.Sprintf(
			"routes__%s",
			strings.TrimPrefix(sanitizedTableOrMeasUUID, "results__"),
		)
		if err := sql.GetRouteScoresOfAddresses(conn, ctx, database, routesTableName); err != nil {
			return err
		}
	}
	return nil
}
