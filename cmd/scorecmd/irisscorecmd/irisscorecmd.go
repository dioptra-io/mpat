package scoreiriscmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/log"
)

var (
	fBefore string
	fAfter  string

	fForceTableReset           bool
	fIrisResearchClickHouseDSN string
	fNumWorkers                int
	fChunkSize                 int

	fRoutesTable string
)

var logger = log.GetLogger()

var ScoreIrisCmd = &cobra.Command{
	Use:   "iris <table-names...>",
	Short: "This script is used to compute the route score for the given tables",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		var resultTableNames []string
		fIrisResearchClickHouseDSN := viper.GetString("iris-research-clickhouse-dsn")

		// Check if the before or after flags are given. If they are given ignore the arguments
		if len(fBefore) != 0 || len(fAfter) != 0 {
			panic("retrieval of the result tables are not supported yet.")
		} else {
			// Get the table names from arguments
			resultTableNames = args
		}

		logger.Infof("Number of results tables to copy is %d, using %d workers.\n", len(resultTableNames), fNumWorkers)

		researchClient := client.FromDSN(fIrisResearchClickHouseDSN)

		// Connect to the prod database
		sqlAdapter, err := researchClient.ClickHouseSQLAdapter(true)
		if err != nil {
			panic(err)
		}

		for i, resultsTableName := range resultTableNames {
			// Get the result table to upload everyting
			newTableName := ConvertResultToRoutesName(resultsTableName)
			precent := float64(i) / float64(len(resultsTableName))
			logger.Infof("Processing [%v/%v %v%%] from %s to %s.\n", i, len(resultTableNames), precent, resultsTableName, newTableName)

			// Get the route traces from the merge
			streamer := client.NewRouteTraceChunkSreamer(sqlAdapter, fChunkSize, "iris", []string{resultsTableName})
			processor := client.NewRouteTraceChunkProcessor(fChunkSize, fNumWorkers)
			uploader, err := client.NewRouteRecordUploader(sqlAdapter, fChunkSize, newTableName, fForceTableReset)
			if err != nil {
				panic(err)
			}

			streamCh, errCh := streamer.Stream()
			streamCh1, errCh1 := processor.Process(streamCh, errCh)
			doneCh, errCh3 := uploader.Upload(streamCh1, errCh1)

			select {
			case _, ok := <-doneCh:
				if ok {
					logger.Debugln("Done processing")
				}
			case err, ok := <-errCh3:
				if ok {
					panic(err)
				}
			}
		}
	},
}

func init() {
	ScoreIrisCmd.PersistentFlags().StringVar(&fBefore, "before", "", "not implemented yet!")
	ScoreIrisCmd.PersistentFlags().StringVar(&fAfter, "after", "", "not implemented yet!")
	ScoreIrisCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", false, "use this to recreate all of the tables")
	ScoreIrisCmd.PersistentFlags().IntVar(&fNumWorkers, "num-workers", 32, "use this to denote number of workers")
	ScoreIrisCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", 10000, "use this to limit the chunk size")
	ScoreIrisCmd.PersistentFlags().StringVar(&fRoutesTable, "routes-table", "", "this is the name of the routes table for easing the computation")

	ScoreIrisCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")

	viper.BindPFlag("iris-prod-clickhouse-dsn", ScoreIrisCmd.Flags().Lookup("iris-prod-clickhouse-dsn"))
	viper.BindPFlag("iris-research-clickhouse-dsn", ScoreIrisCmd.Flags().Lookup("iris-research-clickhouse-dsn"))

	viper.BindEnv("iris-prod-clickhouse-dsn", "MPAT_IRIS_PROD_DSN")
	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
}

func ConvertResultToRoutesName(routesTableName string) string {
	return strings.ReplaceAll(routesTableName, "results", "routes")
}
