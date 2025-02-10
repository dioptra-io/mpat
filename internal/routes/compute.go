package routes

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal"
	"dioptra-io/ufuk-research/internal/sql"
)

var (
	fStdin       bool
	fStopOnError bool
	fNoSQL       bool
)

var RoutesComputeCmd = &cobra.Command{
	Use:   "compute",
	Short: "Create the routes table in the Clickhouse server for each given results table name.",
	Long:  "In the documentation the routes tables are defined as triplets of IPv6 addresses. They correspond to the ip_addr, next_addr, dst_prefix. This program executes an SQL query which creates the table and inserts the calculated values. The values are calculated from results table. If the --stdin flag is set then read the table names from the standard input.",
	Run: func(cmd *cobra.Command, args []string) {
		conn, err := internal.NewConnection()
		if err != nil {
			fmt.Printf("cannot establish connection with Clickhouse %s\n", err)
			return
		}

		// Get the name of the database from the config
		database := viper.GetString("database")

		resultsTableNamesChannel := make(chan string)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				if resultsTableName, ok := <-resultsTableNamesChannel; ok {
					if err := run(conn, database, resultsTableName, fNoSQL); err != nil &&
						fStopOnError {
						fmt.Printf(
							"error while computing the routes table %s, flag --stop-on-error is set to true so exitting\n",
							err,
						)
						break
					} else if err != nil {
						fmt.Printf(
							"error while computing the routes table %s, flag --stop-on-error is set to false so ignoring the error\n",
							err,
						)
					}
				} else {
					fmt.Println("Computation finished!")
					break
				}
			}
		}()

		if fStdin {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				resultsTableNamesChannel <- scanner.Text()
			}
			close(resultsTableNamesChannel)
		} else {
			for _, resultsTableName := range args {
				resultsTableNamesChannel <- resultsTableName
			}
			close(resultsTableNamesChannel)
		}

		wg.Wait()
	},
}

func init() {
	RoutesComputeCmd.PersistentFlags().
		BoolVar(&fStdin, "stdin", false, "set this flag to receive the input from stdin.")
	RoutesComputeCmd.PersistentFlags().
		BoolVar(&fStopOnError, "stop-on-error", false, "set this flag to true if the program should exit if there is an error.")
	RoutesComputeCmd.PersistentFlags().
		BoolVar(&fNoSQL, "no-sql", false, "set this flag to true if the program should streamed data to calculate routes table.")
}

func run(conn clickhouse.Conn, database string, resultsTableName string, noSQL bool) error {
	// No SQL computation of the routes table is not yet supported.
	if noSQL {
		return fmt.Errorf("no-sql is not supported for now")
	} else {
		ctx := context.TODO()
		routesTableName := fmt.Sprintf("routes%s", strings.TrimPrefix(resultsTableName, "results"))

		// Check for table
		if exists, err := sql.CheckTableExists(conn, ctx, database, routesTableName); err != nil {
			return err
		} else if exists {
			return fmt.Errorf("table %s already exists, remove the table to recompute routes table", routesTableName)
		}

		// Create the table
		if err := sql.CreateRoutesTable(conn, ctx, database, routesTableName); err != nil {
			return err
		}

		// Finally select and insert the values
		if err := sql.InsertIntoRoutesFromResults(conn, ctx, database, routesTableName, resultsTableName); err != nil {
			return err
		}

		return nil
	}
}
