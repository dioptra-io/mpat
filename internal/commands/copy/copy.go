package copy

import (
	"math"

	"github.com/spf13/cobra"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var (
	fSourceDSN      string
	fDestinationDSN string

	fParallelDownloads     int
	fChunkSize             int
	fForceResetDestination bool
)

var logger = util.GetLogger()

var CopyCmd = &cobra.Command{
	Use:   "cp <table names>...",
	Short: "Copy given results tables from source to destination",
	Long:  "...",
	Args:  copyIrisArgs,
	Run:   copyIris,
}

func init() {
	// Database related flags
	CopyCmd.Flags().StringVarP(&fSourceDSN, "source-dsn", "s", "", "source database dsn string")
	CopyCmd.Flags().StringVarP(&fDestinationDSN, "destination-dsn", "d", "", "destination database dsn string")

	// Copy related flags
	CopyCmd.Flags().BoolVarP(&fForceResetDestination, "force-reset-destination", "f", config.DefaultForcedResetFlag, "truncate destination tables before copy")
	CopyCmd.Flags().IntVar(&fChunkSize, "chunk-size", config.DefaultChunkSize, "chunk size for the table")
	CopyCmd.Flags().IntVar(&fParallelDownloads, "parallel-downloads", config.DefaultParallelDownloads, "number of concurrent downloads")
}

func copyIrisArgs(cmd *cobra.Command, args []string) error {
	// nop
	return nil
}

func copyIris(cmd *cobra.Command, args []string) {
	resultTablesToCopy := make([]apiv1.ResultsTableInfo, 0)
	totalNumberOfChunks := 0

	// get the clients
	sourceClient, err := clientv1.NewSQLClient(fSourceDSN)
	if err != nil {
		logger.Fatalf("Error while connecting to source database: %v.\n", err)
		return
	}

	destinationClient, err := clientv1.NewSQLClient(fDestinationDSN)
	if err != nil {
		logger.Fatalf("Error while connecting to destination database: %v.\n", err)
		return
	}

	if err := sourceClient.HealthCheck(); err != nil {
		logger.Fatalf("Error on source database healthcheck: %v.\n", err)
		return
	}

	if err := destinationClient.HealthCheck(); err != nil {
		logger.Fatalf("Error on destination database healthcheck: %v.\n", err)
		return
	}

	logger.Infoln("Database healthcheck is successful.")

	// check if the given table names all exist on source.
	sourceTableInfos, err := sourceClient.GetTableInfo(args)
	if err != nil {
		logger.Fatalf("Error while checking source table info: %v.\n", err)
	}

	destinationTableInfos, err := destinationClient.GetTableInfo(args)
	if err != nil {
		logger.Fatalf("Error while checking destination table info: %v.\n", err)
	}

	for i := 0; i < len(args); i++ {
		sourceTableInfo := sourceTableInfos[i]
		destinationTableInfo := destinationTableInfos[i]

		// If it doesnt exist on source, exit
		if !sourceTableInfo.Exists {
			logger.Fatalf("Table does not exists on the source database: %v.\n", sourceTableInfo.TableName)
			return
		}

		// Recreate the destination table
		if !destinationTableInfo.Exists || fForceResetDestination || destinationTableInfo.NumRows != sourceTableInfo.NumRows {
			if err := destinationClient.DropTableIfNotExists(destinationTableInfo.TableName); err != nil {
				logger.Fatalf("Cannot reset destination table: %v.\n", err)
				return
			}
			if err := destinationClient.CreateResultsTableIfNotExists(destinationTableInfo.TableName); err != nil {
				logger.Fatalf("Cannot reset destination table: %v.\n", err)
				return
			}
			logger.Debugf("Created destination table: %v.\n", destinationTableInfo.TableName)

			resultTablesToCopy = append(resultTablesToCopy, sourceTableInfo)                              // add it to copy queue
			totalNumberOfChunks += int(math.Ceil(float64(sourceTableInfo.NumRows) / float64(fChunkSize))) // compte total chunk size
		}
	}

	if len(resultTablesToCopy) == 0 {
		logger.Infoln("All tables already exist on destination, to force table reset use -f flag.")
		return
	} else {
		logger.Infof("Preparing to copy %d table(s) out of %d.\n", len(resultTablesToCopy), len(args))
	}

	// create and start pipeline
	panic("pipeline not implemented")
}
