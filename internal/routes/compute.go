package routes

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal"
)

var (
	fNumWorkers  int
	fProgressBar bool
)

var ComputeCmd = &cobra.Command{
	Use:   "compute",
	Short: "Create the routes table",
	Long:  "The program computes the routes table consisting of triplets (ip_addr, dst_prefix, next_addr) for the given results table name.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Println("Missing argument <results-table-name>")
			return
		}

		resultsTableName := args[0]

		conn, err := internal.NewConnection()
		if err != nil {
			fmt.Printf("Cannot connect to the clickhouse database %s\n", err)
			return
		}

		// Don't forge to close the connection
		defer conn.Close()

		// Check if the table exists
		if exists, err := internal.TableExists(conn, context.TODO(), resultsTableName); err != nil ||
			!exists {
			fmt.Printf("Table exists or cannot connect %s\n", err)
			return
		}

		// Create the table
		routesTableName := internal.ResultsToRoutesTableName(resultsTableName)

		if err := internal.CreateTable(conn, context.TODO(), routesTableName); err != nil {
			fmt.Printf("Cannot create routes table %s: %s\n", routesTableName, err)
			return
		}

		// Insert values into the routes table
		database := viper.GetString("database")
		insertStatement := internal.SQLInsertIntoRoutes(database, resultsTableName)

		if err := conn.Exec(context.TODO(), insertStatement); err != nil {
			fmt.Printf(
				"Cannot insert to routes table from results table %s: %s\n",
				routesTableName,
				err,
			)
			return
		}
	},
}
