package copyarkcmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/log"
)

var logger = log.GetLogger()

var (
	fBefore          string
	fAfter           string
	fIrisAPIUser     string
	fIrisAPIPassword string
	fIrisAPIUrl      string

	fIrisResearchClickHouseDSN string

	fParallelDownloads int
	fForceTableReset   bool
	fChunkSize         int
)

var CopyArkCmd = &cobra.Command{
	Use:   "ark <datetimes...>",
	Short: "This command is used to copy the Ark data.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		var datesToFetch []string
		fIrisResearchClickHouseDSN := viper.GetString("iris-research-clickhouse-dsn")

		// Check if the before or after flags are given. If they are given ignore the arguments
		if len(fBefore) != 0 || len(fAfter) != 0 {
			panic("retrieval of the result tables are not supported yet.")
		} else {
			// Get the table names from arguments
			datesToFetch = args
		}

		researchClient := client.FromDSN(fIrisResearchClickHouseDSN)
		if _, err := researchClient.ClickHouseSQLAdapter(true); err != nil {
			panic(err)
		}

		logger.Infof("Connected to databases.\n")
		logger.Infof("Number of dates to copy is %d, using %d workers.\n", len(datesToFetch), fParallelDownloads)

		panic("not implemented")
	},
}

func init() {
	CopyArkCmd.PersistentFlags().StringVar(&fBefore, "before", "", "before datetime")
	CopyArkCmd.PersistentFlags().StringVar(&fAfter, "after", "", "after datetime")
	CopyArkCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", false, "use this to recreate all of the tables")
	CopyArkCmd.PersistentFlags().IntVar(&fParallelDownloads, "parallel-downloads", 32, "use this to limit number of concurrent downloads")

	CopyArkCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")
	viper.BindPFlag("iris-research-clickhouse-dsn", CopyArkCmd.Flags().Lookup("iris-research-clickhouse-dsn"))
	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
}
