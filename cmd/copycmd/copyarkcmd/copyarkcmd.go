package copyarkcmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/log"
)

// This is defined to put and pass values to the functions using context.Context.
// The key of the context as a string is not best practice as it can suffer from
// name colisions.
type contextKey string

var keyForceTableReset contextKey = "keyForceTableReset"

var logger = log.GetLogger()

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

var CopyArkCmd = &cobra.Command{
	Use:   "ark <table-names...>",
	Short: "This command is used to copy the Iris data.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	CopyArkCmd.PersistentFlags().StringVar(&fBefore, "before", "", "not implemented yet!")
	CopyArkCmd.PersistentFlags().StringVar(&fAfter, "after", "", "not implemented yet!")
	CopyArkCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", false, "use this to recreate all of the tables")
	CopyArkCmd.PersistentFlags().IntVar(&fParallelDownloads, "parallel-downloads", 32, "use this to limit number of concurrent downloads")
	CopyArkCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", 100000, "use this to limit the chunk size")

	CopyArkCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")

	viper.BindPFlag("iris-research-clickhouse-dsn", CopyArkCmd.Flags().Lookup("iris-research-clickhouse-dsn"))

	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
}
