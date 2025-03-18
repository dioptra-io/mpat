package copy

import (
	"context"
	"math"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	clientv1 "dioptra-io/ufuk-research/pkg/client/v1"
	"dioptra-io/ufuk-research/pkg/query"
	"dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

var fIrisProdClickHouseDSN string

var CopyIrisCmd = &cobra.Command{
	Use:   "iris <table-names...>",
	Short: "This command is used to copy the Iris data.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		var resultTableNames []string
		fIrisProdClickHouseDSN := viper.GetString("iris-prod-clickhouse-dsn")
		fIrisResearchClickHouseDSN := viper.GetString("iris-research-clickhouse-dsn")

		// Check if the before or after flags are given. If they are given ignore the arguments
		if len(fBefore) != 0 || len(fAfter) != 0 {
			panic("retrieval of the result tables are not supported yet.")
		} else {
			// Get the table names from arguments
			resultTableNames = args
		}

		logger.Infof("Number of results tables to copy is %d, using %d workers.\n", len(resultTableNames), fParallelDownloads)

		irisProdCHClient, err := clientv1.NewClickHouseClient(fIrisProdClickHouseDSN)
		if err != nil {
			panic(err)
		}
		irisResearchCHClient, err := clientv1.NewClickHouseClient(fIrisResearchClickHouseDSN)
		if err != nil {
			panic(err)
		}

		irisProdHTTPCHClient, err := clientv1.NewHTTPClickHouseClient(fIrisProdClickHouseDSN)
		if err != nil {
			panic(err)
		}
		irisResearchHTTPCHClient, err := clientv1.NewHTTPClickHouseClient(fIrisResearchClickHouseDSN)
		if err != nil {
			panic(err)
		}

		logger.Infof("Connected to databases.\n")

		rowsPerTable, err := getRowsPerTable(irisProdCHClient, resultTableNames)
		if err != nil {
			panic(err)
		}

		// This is just for displaying proper values
		totalNumberOfChunks := 0
		finishedNumberOfChunks := 0
		chunksPerTable := make([]int, len(resultTableNames))
		for i, rows := range rowsPerTable {
			numChunks := int(math.Ceil(float64(rows) / float64(fChunkSize)))
			chunksPerTable[i] = numChunks
			totalNumberOfChunks += numChunks
		}

		logger.Infof("Total of %d chunk(s) from total of %d tables will be processed.\n", totalNumberOfChunks, len(resultTableNames))
		// Create the context
		ctx := context.WithValue(context.Background(), keyForceTableReset, fForceTableReset)

		// Run the tables sequentially but chunks in parallel
		for i, tableName := range args {
			logger.Infof("Preparing for table %s.\n", tableName)
			if err := copyTable(ctx,
				irisProdCHClient,
				irisResearchCHClient,
				irisProdHTTPCHClient,
				irisResearchHTTPCHClient,
				tableName,
				finishedNumberOfChunks,
				totalNumberOfChunks,
				chunksPerTable[i]); err != nil {
				panic(err)
			}
			finishedNumberOfChunks += chunksPerTable[i]
		}
	},
}

func init() {
	CopyIrisCmd.PersistentFlags().StringVar(&fBefore, "before", "", "not implemented yet!")
	CopyIrisCmd.PersistentFlags().StringVar(&fAfter, "after", "", "not implemented yet!")
	CopyIrisCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", false, "use this to recreate all of the tables")
	CopyIrisCmd.PersistentFlags().IntVar(&fParallelDownloads, "parallel-downloads", 32, "use this to limit number of concurrent downloads")
	CopyIrisCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", 100000, "use this to limit the chunk size")

	CopyIrisCmd.PersistentFlags().StringVar(&fIrisProdClickHouseDSN, "iris-prod-clickhouse-dsn", "", "DSN string for prod")
	CopyIrisCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")

	viper.BindPFlag("iris-prod-clickhouse-dsn", CopyIrisCmd.Flags().Lookup("iris-prod-clickhouse-dsn"))
	viper.BindPFlag("iris-research-clickhouse-dsn", CopyIrisCmd.Flags().Lookup("iris-research-clickhouse-dsn"))

	viper.BindEnv("iris-prod-clickhouse-dsn", "MPAT_IRIS_PROD_DSN")
	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
}

func getRowsPerTable(prodCHClient client.DBClient, tableNames []string) ([]int, error) {
	rows := make([]int, len(tableNames))

	for i, tableName := range tableNames {
		var count int
		if err := prodCHClient.QueryRow(query.SelectCount(tableName)).Scan(&count); err != nil {
			return nil, err
		}
		rows[i] = count
	}

	return rows, nil
}

func copyTable(ctx context.Context,
	prodCHClient,
	researchCHClient client.DBClient,
	prodHTTPCHClient,
	researchHTTPCHClient client.HTTPDBClient,
	tableName string,
	finishedNumberOfChunks,
	totalNumberOfChunks,
	chunks int,
) error {
	// Get the parameters from context
	forceTableReset := ctx.Value(keyForceTableReset).(bool)

	// Drop the table on the research if the flag forceTableReset is set.
	if forceTableReset {
		if _, err := researchCHClient.Exec(query.DropTable(tableName, true)); err != nil {
			return err
		}
	}

	// Create the table if not exists
	if _, err := researchCHClient.Exec(query.CreateResultsTable(tableName, true)); err != nil {
		return err
	}

	// Get number of rows from the resarch server
	var researchRows int
	if err := researchCHClient.QueryRow(query.SelectCount(tableName)).Scan(&researchRows); err != nil {
		return err
	}

	if researchRows != 0 {
		logger.Info("There is already data in the research instance! Skipping...\n")
		return nil
	}

	// For performance optimization we are doing this using curl/http as a requests
	// instead of scanning the rows.
	lock := sync.Mutex{}
	localOrder := 0

	var wg sync.WaitGroup
	rateLimitedCh := make(chan int, fParallelDownloads)

	for j := 0; j < chunks; j++ {
		offset := fChunkSize * j

		wg.Add(1)
		rateLimitedCh <- j

		go func() {
			logger.Debugf("Started worker %v.\n", j)
			if err := copyChunk(
				prodHTTPCHClient,
				researchHTTPCHClient,
				tableName,
				offset,
			); err != nil {
				<-rateLimitedCh
				wg.Done()
				return
			}
			<-rateLimitedCh
			wg.Done()

			lock.Lock()
			localOrder += 1
			lock.Unlock()

			percent := 100 * float64(localOrder) / float64(chunks)
			totalPercent := 100 * float64(localOrder+finishedNumberOfChunks) / float64(totalNumberOfChunks)
			logger.Infof("Processed chunk (%v/%v %.2f%%)[%v/%v %.2f%%] for table %s.\n",
				localOrder+finishedNumberOfChunks,
				totalNumberOfChunks,
				totalPercent,
				localOrder,
				chunks,
				percent,
				tableName)
		}()

	}

	wg.Wait()

	return nil
}

func copyChunk(prodHTTP, researchHTTP client.HTTPDBClient, tableName string, offset int) error {
	dataFormat := "Native"
	selectQuery := query.SelectLimitOffsetFormat(tableName, fChunkSize, offset, dataFormat)
	insertQuery := query.InsertFormat(tableName, dataFormat)

	downloader, err := prodHTTP.Download(selectQuery)
	if err != nil {
		return err
	}

	uploader, err := researchHTTP.Upload(insertQuery, downloader)
	if err != nil {
		panic(err)
	}

	defer downloader.Close()
	defer uploader.Close()

	return nil
}
