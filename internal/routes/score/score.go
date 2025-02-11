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

		if err := runScores(conn, database, args[0], false); err != nil {
			fmt.Print(err)
			return
		}
	},
}

func runScores(
	conn clickhouse.Conn,
	database string,
	inputTableorUUID string,
	inputUUID bool,
) error {
	ctx := context.TODO()
	routesTableName := fmt.Sprintf("routes%s", strings.TrimPrefix(inputTableorUUID, "results"))
	// No SQL computation of the routes table is not yet supported.
	if inputUUID {
		if err := sql.GetRouteScoresOfAddressesMerged(conn, ctx, database, routesTableName); err != nil {
			return err
		}
	} else {
		if err := sql.GetRouteScoresOfAddresses(conn, ctx, database, routesTableName); err != nil {
			return err
		}
	}
	return nil
}
