package copyiriscmd

import (
	"context"
	"math"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/query"
	"dioptra-io/ufuk-research/pkg/util"
)

// This is defined to put and pass values to the functions using context.Context.
// The key of the context as a string is not best practice as it can suffer from
// name colisions.
type contextKey string

var keyForceTableReset contextKey = "keyForceTableReset"

var logger = util.GetLogger()

var (
	fBefore          string
	fAfter           string
	fIrisAPIUser     string
	fIrisAPIPassword string
	fIrisAPIUrl      string

	fIrisProdClickHouseDSN     string
	fIrisResearchClickHouseDSN string

	fParallelDownloads int
	fForceTableReset   bool
	fChunkSize         int
)

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

		prodClient := client.FromDSN(fIrisProdClickHouseDSN)
		researchClient := client.FromDSN(fIrisResearchClickHouseDSN)

		// Connect to the prod database
		if _, err := prodClient.ClickHouseSQLAdapter(true); err != nil {
			panic(err)
		}
		// Connect to the research database
		if _, err := researchClient.ClickHouseSQLAdapter(true); err != nil {
			panic(err)
		}

		logger.Infof("Connected to databases.\n")

		rowsPerTable, err := getRowsPerTable(prodClient, resultTableNames)
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
			if err := copyTable(ctx, prodClient, researchClient, tableName, finishedNumberOfChunks, totalNumberOfChunks, chunksPerTable[i]); err != nil {
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

func getRowsPerTable(prodClient client.IrisClient, tableNames []string) ([]int, error) {
	prodSQLAdapter, err := prodClient.ClickHouseSQLAdapter(false)
	if err != nil {
		return nil, err
	}

	rows := make([]int, len(tableNames))

	for i, tableName := range tableNames {
		var count int
		if err := prodSQLAdapter.QueryRow(query.SelectCount(tableName)).Scan(&count); err != nil {
			return nil, err
		}
		rows[i] = count
	}

	return rows, nil
}

func copyTable(ctx context.Context, prodClient, researchClient client.IrisClient, tableName string, finishedNumberOfChunks, totalNumberOfChunks, chunks int) error {
	// Get the parameters from context
	forceTableReset := ctx.Value(keyForceTableReset).(bool)

	// Get other values
	prodHTTPAdapter, err := prodClient.ClickHouseHTTPAdapter(false)
	if err != nil {
		return err
	}

	researchSQLAdapter, err := researchClient.ClickHouseSQLAdapter(false)
	if err != nil {
		return err
	}

	researchHTTPAdapter, err := researchClient.ClickHouseHTTPAdapter(false)
	if err != nil {
		return err
	}

	// Drop the table on the research if the flag forceTableReset is set.
	if forceTableReset {
		if _, err := researchSQLAdapter.Exec(query.DropTable(tableName, true)); err != nil {
			return err
		}
	}

	// Create the table if not exists
	if _, err := researchSQLAdapter.Exec(query.CreateResultsTable(tableName, true)); err != nil {
		return err
	}

	// Get number of rows from the resarch server
	var researchRows int
	if err := researchSQLAdapter.QueryRow(query.SelectCount(tableName)).Scan(&researchRows); err != nil {
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
				prodHTTPAdapter,
				researchHTTPAdapter,
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

func copyChunk(prodHTTP, researchHTTP client.ClickHouseHTTPAdapter, tableName string, offset int) error {
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
