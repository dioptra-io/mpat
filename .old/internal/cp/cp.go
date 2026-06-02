package cp

import (
	"bufio"
	"context"
	"errors"
	"math"
	"os"
	"slices"
	"time"

	"github.com/spf13/cobra"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	"github.com/dioptra-io/ufuk-research/internal/pipeline"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

// flags
var (
	fSourceDSN      string
	fDestinationDSN string

	fParallelDownloads     int
	fChunkSize             int
	fMaxRowUploadRate      int
	fForceResetDestination bool
	fInputFile             string
	fMaxRetries            int
)

var inputTableNames []string

var logger = util.GetLogger()

func CpCmd() *cobra.Command {
	cpCmd := &cobra.Command{
		Use:   "cp <tablenames>...",
		Short: "Copy given results tables from source to destination",
		Long:  "This command first creates the tables that are not on the destination, then chunks from the source and inserts into destnation",
		Args:  cpArgs,
		Run:   cp,
	}
	// Database related flags
	cpCmd.Flags().StringVarP(&fSourceDSN, "source-dsn", "s", "", "source database dsn string")
	cpCmd.Flags().StringVarP(&fDestinationDSN, "destination-dsn", "d", "", "destination database dsn string")

	// Copy related flags
	cpCmd.Flags().BoolVarP(&fForceResetDestination, "force-reset-destination", "f", config.DefaultForcedResetFlag, "truncate destination tables before copy")
	cpCmd.Flags().IntVar(&fChunkSize, "chunk-size", config.DefaultChunkSize, "chunk size for the table")
	cpCmd.Flags().IntVar(&fParallelDownloads, "parallel-downloads", config.DefaultParallelDownloads, "number of concurrent downloads")
	cpCmd.Flags().StringVar(&fInputFile, "input-file", "", "file to read the table names")
	cpCmd.Flags().IntVarP(&fMaxRowUploadRate, "max-row-upload-rate", "r", config.DefaultMaxRowUploadRate, "limit the number of rows to upload per second")
	cpCmd.Flags().IntVar(&fMaxRetries, "max-retries", config.DefaultMaxRetries, "number of retries if a downlaod or upload fails")

	return cpCmd
}

func cpArgs(cmd *cobra.Command, args []string) error {
	if fInputFile == "" {
		if len(args) == 0 {
			cmd.Help()
			os.Exit(0)
		}

		inputTableNames = args
	} else {
		if len(args) != 0 {
			return errors.New("cannot specify both input file and arguments")
		}
		f, err := os.Open(fInputFile)
		if err != nil {
			return err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		inputTableNames = make([]string, 0)
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			inputTableNames = append(inputTableNames, scanner.Text())
		}

	}

	slices.Sort(inputTableNames) // sort for easy compuation

	logger.Infof("Preparing to process %d table(s).\n", len(inputTableNames))

	return nil
}

func cp(cmd *cobra.Command, args []string) {
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
	sourceTableInfos, err := sourceClient.GetTableInfo(inputTableNames)
	if err != nil {
		logger.Fatalf("Error while checking source table info: %v.\n", err)
	}

	destinationTableInfos, err := destinationClient.GetTableInfo(inputTableNames)
	if err != nil {
		logger.Fatalf("Error while checking destination table info: %v.\n", err)
	}

	for i := 0; i < len(inputTableNames); i++ {
		sourceTableInfo := sourceTableInfos[i]
		if !sourceTableInfo.Exists { // if given table does not exists on source
			logger.Fatalf("Table does not exists on the source database: %v.\n", sourceTableInfo.TableName)
			return
		}
	}

	logger.Infoln("Validating tables on destination.")
	resultTablesToCopy := make([]apiv1.ResultsTableInfo, 0)
	totalNumberOfChunks := 0
	numTablesToCopy := 0

	for i := 0; i < len(inputTableNames); i++ {
		sourceTableInfo := sourceTableInfos[i]
		destinationTableInfo := destinationTableInfos[i]

		logger.Debugf("here dst.exists=%v, dst.count=%d, src.count=%d, force=%v.\n", destinationTableInfo.Exists, destinationTableInfo.NumRows, sourceTableInfo.NumRows, fForceResetDestination)

		if !destinationTableInfo.Exists || fForceResetDestination || destinationTableInfo.NumRows != sourceTableInfo.NumRows {
			if destinationTableInfo.Exists && !fForceResetDestination {
				if err := destinationClient.TruncateTableIfNotExists(destinationTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset destination table: %v.\n", err)
					return
				}
			} else {
				if err := destinationClient.DropTableIfNotExists(destinationTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset destination table: %v.\n", err)
					return
				}
				if err := destinationClient.CreateResultsTableIfNotExists(destinationTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset destination table: %v.\n", err)
					return
				}
			}
			logger.Debugf("Created destination table: %v.\n", destinationTableInfo.TableName)

			resultTablesToCopy = append(resultTablesToCopy, sourceTableInfo)                              // add it to copy queue
			totalNumberOfChunks += int(math.Ceil(float64(sourceTableInfo.NumRows) / float64(fChunkSize))) // compte total chunk size
			numTablesToCopy++
		}
	}

	if len(resultTablesToCopy) == 0 {
		logger.Infof("Validated %d tables(s), all on destination are already valid. Exitting.\n", len(inputTableNames))
		return
	} else {
		logger.Infof("Validated %d tables(s), %d of them will be copied using total of %d chunks.\n", len(inputTableNames), numTablesToCopy, totalNumberOfChunks)
	}

	// create and start pipeline
	pipeline, errCh, err := pipeline.NewCopyPipeline(sourceClient, destinationClient, resultTablesToCopy, fParallelDownloads, fChunkSize, fMaxRowUploadRate, fMaxRetries)
	if err != nil {
		logger.Fatalf("Error while preaparing the copy pipeline: %v.\n", err)
		return
	}
	defer pipeline.Close() // close the pipeline after

	pipeline.Start(context.Background())

	startTime := time.Now()

	processedNumChunks := 0
	for processedChunks := range pipeline.Output() {
		processedNumChunks++
		percent := 100 * float64(processedNumChunks) / float64(totalNumberOfChunks)
		elapsed := time.Since(startTime).Truncate(time.Second)
		eta := ((elapsed / time.Duration(processedNumChunks)) * time.Duration(totalNumberOfChunks-processedNumChunks)).Truncate(time.Second)
		logger.Infof("Processed [%.2f%%, elapsed=%v, eta=%v] chunk %d / %d for table %s.\n", percent, elapsed, eta, processedNumChunks, totalNumberOfChunks, processedChunks.Info.TableName)
	}

	done := false
	numPipelineFails := 0
	for !done {
		select {
		case err := <-errCh:
			logger.Fatalf("Pipeline failed with error: %v.\n", err)
			numPipelineFails++
		default:
			done = true
		}
	}

	if numPipelineFails != 0 {
		logger.Infof("Failed to copy for %d table(s) with %d error(s).\n", numTablesToCopy, numPipelineFails)
	} else {
		logger.Infof("Finished copying for %d table(s).\n", numTablesToCopy)
	}
}
