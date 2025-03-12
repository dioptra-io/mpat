package score

import (
	"io"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/common"
	"dioptra-io/ufuk-research/internal/log"
)

var (
	fOutput         string
	fNoSQL          bool
	fForceRecompute bool
	fAddresses      []string
	fAgentUUIDs     []string
)

var logger = log.GetLogger()

var IrisScoreCmd = &cobra.Command{
	Use:   "score ",
	Short: "Compute the routes table and get the routes score.",
	Long:  "...",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
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
	logger.Debugf(
		"Received %v existing tables, and %v non-existing tables.\n",
		len(existingRouteTables),
		len(nonExistingRouteTables),
	)
	logger.Debugf(
		"Existing route tables: %v.\n",
		existingRouteTables,
	)
	logger.Debugf(
		"Nonexisting route tables: %v.\n",
		nonExistingRouteTables,
	)

	tableNamesToProcess := nonExistingRouteTables

	// Recompute all of the table names if the flag is set.
	if cfg.ForceRecompute {
		tableNamesToProcess = append(
			tableNamesToProcess,
			existingRouteTables...)
		logger.Debugf(
			"Force recompute flag is set, recomputing total of %v tables.\n",
			len(tableNamesToProcess),
		)
	}

	// Create the nonexisting tables
	if err := common.CreateRouteTablesIfNotExists(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	logger.Infof("Created %v route table(s).\n", len(tableNamesToProcess))

	// Truncate and populate the routes table
	if err := common.TruncateTables(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	logger.Infof("Truncated %v route table(s).\n", len(tableNamesToProcess))

	// Compute the routes table
	if err := common.ComputeRouteTables(conn, cfg.Database, tableNamesToProcess); err != nil {
		return err
	}

	logger.Debugln("Computation for the routes tables are done!")
	logger.Debugln("Ready to create compute the scores")

	// Get the names of all route tables
	allRouteTables := append(existingRouteTables, nonExistingRouteTables...)

	// Print the scores of the addresses to the io.Writer
	if err := common.GetScoresFromRouteTables(conn, cfg.Database, allRouteTables, cfg.Addresses, cfg.OutputWriter); err != nil {
		return err
	}

	logger.Debugln("Computation for the scores tables are done!")

	return nil
}
