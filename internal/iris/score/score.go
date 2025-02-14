package score

import (
	"fmt"
	"io"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/common"
)

var (
	fOutput         string
	fNoSQL          bool
	fForceRecompute bool
	fAddresses      []string
	fAgentUUIDs     []string
)

var IrisScoreCmd = &cobra.Command{
	Use:   "score measurement-uuid [agent-uuid]",
	Short: "Compute the routes table and get the routes score.",
	Long:  "...",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if fNoSQL {
			fmt.Println("--no-sql flag is set however it is not supported.")
			return
		}

		conn, err := common.NewConnection()
		if err != nil {
			fmt.Printf("cannot establish connection with Clickhouse %s\n", err)
			return
		}

		// Create the output writer
		var writer io.Writer

		if fOutput == "" {
			writer = os.Stdout
		} else {
			file, err := os.OpenFile(fOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Printf("cannot open output file %s\n", fOutput)
				return
			}

			defer file.Close()

			writer = file
		}

		// Create the configuration for running the command.
		cfg := &irisScoreConfig{
			Database:        viper.GetString("database"),
			ForceRecompute:  fForceRecompute,
			NoSQL:           fNoSQL,
			Addresses:       fAddresses,
			AgentUUIds:      fAgentUUIDs,
			OutputWriter:    writer,
			MeasurementUUID: args[0],
		}

		if err := run(conn, cfg); err != nil {
			fmt.Print(err)
			return
		}
	},
}

type irisScoreConfig struct {
	Database        string
	ForceRecompute  bool
	NoSQL           bool
	Addresses       []string
	AgentUUIds      []string
	OutputWriter    io.Writer
	MeasurementUUID string
}

func init() {
	IrisScoreCmd.Flags().
		StringVarP(&fOutput, "output", "o", "", "set this flag to a filename to specify output filename, default is empty (stdout).")
	IrisScoreCmd.Flags().
		BoolVar(&fNoSQL, "no-sql", false, "(not implemented yet) if set to true the processing is done by the program, else by Clickhouse.")
	IrisScoreCmd.Flags().
		BoolVar(&fForceRecompute, "force-recompute", false, "recompute the route tables event though they already exist.")
	IrisScoreCmd.Flags().
		StringArrayVar(&fAddresses, "addresses", []string{}, "specify which ip addresses to not filter out, for all addresses set this to empty.")
	IrisScoreCmd.Flags().
		StringArrayVar(&fAgentUUIDs, "agent-uuids", []string{}, "specify which agents to retrieve data, for all agents set this to empty.")
}

// This is the main function that performs the operation. It is a pure function.
func run(
	conn clickhouse.Conn,
	cfg *irisScoreConfig,
) error {
	// Check if the tables exists
	var existingRouteTables []string
	var nonExistingRouteTables []string
	var err error

	if existingRouteTables, nonExistingRouteTables, err = common.GetExistingAndNonExistingTables(
		conn,
		cfg.Database,
		cfg.MeasurementUUID,
		cfg.AgentUUIds,
	); err != nil {
		return err
	}

	tableNamesToProcess := nonExistingRouteTables

	// Recompute all of the table names if the flag is set.
	if cfg.ForceRecompute {
		tableNamesToProcess = append(
			tableNamesToProcess,
			existingRouteTables...)
	}

	// Create the nonexisting tables
	if err := common.CreateRouteTablesIfNotExists(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	// Truncate and populate the routes table
	if err := common.TruncateTables(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	// Compute the routes table
	if err := common.ComputeRouteTables(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	// Get the names of all route tables
	allRouteTables := append(existingRouteTables, nonExistingRouteTables...)

	// Print the scores of the addresses to the io.Writer
	if err := common.GetScoresFromRouteTables(conn, cfg.Database, allRouteTables, cfg.Addresses, cfg.OutputWriter); err != nil {
		return err
	}

	return nil
}
