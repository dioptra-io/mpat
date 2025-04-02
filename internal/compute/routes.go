package compute

import (
	"bufio"
	"context"
	"errors"
	"os"
	"slices"

	"github.com/spf13/cobra"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	"github.com/dioptra-io/ufuk-research/internal/pipeline"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

// flags
var (
	fSourceDSN             string
	fMaxNumWorkers         int
	fMaxRowUploadRate      int
	fForceResetDestination bool
	fInputFile             string
	fMaxRetries            int
)

var (
	operation       string
	inputTableNames []string
)

var logger = util.GetLogger()

func ComputeCmd() *cobra.Command {
	computeCmd := &cobra.Command{
		Use:   "compute <command>",
		Short: "Compute the given tables to requested.",
		Long:  "Compute table types.",
		Args:  computeCmdArgs,
		Run:   computeCmd,
	}
	computeCmd.PersistentFlags().StringVarP(&fSourceDSN, "source-dsn", "s", "", "source database dsn string")
	computeCmd.PersistentFlags().BoolVarP(&fForceResetDestination, "force-reset-destination", "f", config.DefaultForcedResetFlag, "truncate destination tables before copy")
	computeCmd.PersistentFlags().StringVar(&fInputFile, "input-file", "", "file to read the table names")
	computeCmd.PersistentFlags().IntVarP(&fMaxRowUploadRate, "max-row-upload-rate", "r", config.DefaultMaxRowUploadRate, "limit the number of rows to upload per second")
	computeCmd.PersistentFlags().IntVar(&fMaxRetries, "max-retries", config.DefaultMaxRetries, "number of retries if a downlaod or upload fails")

	computeRoutesCmd := &cobra.Command{
		Use:   "routes <table names>...",
		Short: "Compute the routes table from results tables.",
		Long:  "Compute the route tables from arguments or input file.",
		Args:  computeCmdArgs,
		Run:   computeRoutesCmd,
	}

	computeCmd.AddCommand(computeRoutesCmd)

	return computeCmd
}

func computeCmdArgs(cmd *cobra.Command, args []string) error {
	if fInputFile == "" {
		if len(args) <= 0 {
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

func computeCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func computeRoutesCmd(cmd *cobra.Command, args []string) {
	// check validity of the table names
	resultsTables := make([]apiv1.TableName, len(inputTableNames))
	routesTables := make([]apiv1.TableName, len(inputTableNames))
	for i, name := range inputTableNames {
		resutlsTable := apiv1.TableName(name)
		if resutlsTable.Type() != apiv1.ResultsTable {
			logger.Fatalf("Erro while validating table names: given table is not a results table: %s.\n", name)
			return
		}
		resultsTables[i] = resutlsTable
		if routesTable, err := resutlsTable.Convert(apiv1.RoutesTable); err != nil {
			logger.Fatalf("Erro while validating table names: given resutls table is not translatable to routes table: %s.\n", name)
			return
		} else {
			routesTables[i] = routesTable
		}
	}

	// get the clients
	sourceClient, err := clientv1.NewSQLClient(fSourceDSN)
	if err != nil {
		logger.Fatalf("Error while connecting to source database: %v.\n", err)
		return
	}

	if err := sourceClient.HealthCheck(); err != nil {
		logger.Fatalf("Error on source database healthcheck: %v.\n", err)
		return
	}

	logger.Infoln("Database healthcheck is successful.")

	// check if the given table names all exist on source.
	resultsTableInfos, err := sourceClient.GetTableInfoFromTableName(resultsTables)
	if err != nil {
		logger.Fatalf("Error while checking source table info: %v.\n", err)
	}

	routesTableInfos, err := sourceClient.GetTableInfoFromTableName(routesTables)
	if err != nil {
		logger.Fatalf("Error while checking destination table info: %v.\n", err)
	}

	for i := 0; i < len(resultsTableInfos); i++ {
		resultsTableInfo := resultsTableInfos[i]
		if !resultsTableInfo.Exists { // if given table does not exists on source
			logger.Fatalf("Table does not exists on the source database: %v.\n", resultsTableInfo.TableName)
			return
		}
	}
	logger.Infoln("Validating route tables.")
	resultTablesToProcess := make([]apiv1.ResultsTableInfo, 0)
	numTablesToCopy := 0

	for i := 0; i < len(inputTableNames); i++ {
		resultsTableInfo := resultsTableInfos[i]
		routesTableInfo := routesTableInfos[i]

		logger.Debugf("resutls.exists=%v, results.count=%d, routes.exists=%v, force=%v.\n", resultsTableInfo.Exists, resultsTableInfo.NumRows, routesTableInfo.Exists, fForceResetDestination)

		// criteria for recomputation, not exsits or forced or size 0
		if !routesTableInfo.Exists || fForceResetDestination || routesTableInfo.NumRows == 0 {
			if routesTableInfo.Exists && !fForceResetDestination {
				if err := sourceClient.TruncateTableIfNotExists(routesTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset routes table: %v.\n", err)
					return
				}
			} else {
				if err := sourceClient.DropTableIfNotExists(routesTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset routes table: %v.\n", err)
					return
				}
				if err := sourceClient.CreateRoutesTableIfNotExists(routesTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset routes table: %v.\n", err)
					return
				}
			}
			logger.Debugf("Created routes table: %v.\n", routesTableInfo.TableName)

			resultTablesToProcess = append(resultTablesToProcess, resultsTableInfo) // add it to copy queue
			numTablesToCopy++
		}
	}

	if len(resultTablesToProcess) == 0 {
		logger.Infof("Validated %d tables(s), all on destination are already valid. Exitting.\n", len(inputTableNames))
		return
	} else {
		logger.Infof("Validated %d tables(s), %d of them will be processed.\n", len(inputTableNames), numTablesToCopy)
	}

	routesPipeline, err := pipeline.NewRoutesPipeline(sourceClient, routesTables, pipeline.RoutesPipelineConfig{
		NumWorkers:      2,
		NumUploaders:    1,
		NumMaxRetries:   3,
		MaxUploadRate:   0,
		UploadChunkSize: 10000,
	})
	if err != nil {
		logger.Fatalf("Error occured while creating the routes pipeline: %v.\n", err)
		return
	}
	defer routesPipeline.Close()

	if err := routesPipeline.Start(context.Background()); err != nil {
		logger.Fatalf("Error occured while starting the routes pipeline: %v.\n", err)
		return
	}

	logger.Debugln("Started pipeline")

	for processedChunks := range routesPipeline.OutCh() {
		logger.Debugf("zort: %v", processedChunks)
	}

	done := false
	numPipelineFails := 0
	for !done {
		select {
		case err := <-routesPipeline.ErrCh():
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

	// pipeline, errCh, err := pipeline.NewRoutesPipeline(sourceClient)
	// // create and start pipeline
	// pipeline, errCh, err := pipeline.NewCopyPipeline(sourceClient, destinationClient, resultTablesToCopy, fParallelDownloads, fChunkSize, fMaxRowUploadRate, fMaxRetries)
	// if err != nil {
	// 	logger.Fatalf("Error while preaparing the copy pipeline: %v.\n", err)
	// 	return
	// }
	// defer pipeline.Close() // close the pipeline after
	//
	// pipeline.Start(context.Background())
	//
	// startTime := time.Now()
	//
	// processedNumChunks := 0
	// for processedChunks := range pipeline.Output() {
	// 	processedNumChunks++
	// 	percent := 100 * float64(processedNumChunks) / float64(totalNumberOfChunks)
	// 	elapsed := time.Since(startTime).Truncate(time.Second)
	// 	eta := ((elapsed / time.Duration(processedNumChunks)) * time.Duration(totalNumberOfChunks-processedNumChunks)).Truncate(time.Second)
	// 	logger.Infof("Processed [%.2f%%, elapsed=%v, eta=%v] chunk %d / %d for table %s.\n", percent, elapsed, eta, processedNumChunks, totalNumberOfChunks, processedChunks.Info.TableName)
	// }
	//
	// done := false
	// numPipelineFails := 0
	// for !done {
	// 	select {
	// 	case err := <-errCh:
	// 		logger.Fatalf("Pipeline failed with error: %v.\n", err)
	// 		numPipelineFails++
	// 	default:
	// 		done = true
	// 	}
	// }
	//
	// if numPipelineFails != 0 {
	// 	logger.Infof("Failed to copy for %d table(s) with %d error(s).\n", numTablesToCopy, numPipelineFails)
	// } else {
	// 	logger.Infof("Finished copying for %d table(s).\n", numTablesToCopy)
	// }
}
