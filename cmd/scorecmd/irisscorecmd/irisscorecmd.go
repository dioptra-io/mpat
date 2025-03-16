package irisscorecmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/log"
)

var (
	fBefore string
	fAfter  string

	fIrisResearchClickHouseDSN string
	fNumWorkers                int
	fChunkSize                 int
)

var logger = log.GetLogger()

var IrisScoreCmd = &cobra.Command{
	Use:   "score <table-names...>",
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
		if _, err := researchClient.ClickHouseSQLAdapter(true); err != nil {
			panic(err)
		}

		// Get the route traces from the merge
	},
}

func init() {
	IrisScoreCmd.PersistentFlags().StringVar(&fBefore, "before", "", "not implemented yet!")
	IrisScoreCmd.PersistentFlags().StringVar(&fAfter, "after", "", "not implemented yet!")
	IrisScoreCmd.PersistentFlags().IntVar(&fNumWorkers, "num-workers", 32, "use this to denote number of workers")
	IrisScoreCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", 1000, "use this to limit the chunk size")

	IrisScoreCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")

	viper.BindPFlag("iris-prod-clickhouse-dsn", IrisScoreCmd.Flags().Lookup("iris-prod-clickhouse-dsn"))
	viper.BindPFlag("iris-research-clickhouse-dsn", IrisScoreCmd.Flags().Lookup("iris-research-clickhouse-dsn"))

	viper.BindEnv("iris-prod-clickhouse-dsn", "MPAT_IRIS_PROD_DSN")
	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
}
