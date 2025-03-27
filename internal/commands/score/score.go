package score

// import (
// 	"io"
// 	"os"
//
// 	"github.com/spf13/cobra"
// 	"github.com/spf13/viper"
//
// 	"dioptra-io/ufuk-research/pkg/config"
// )
//
// var (
// 	// Before and after flag used to retirieve the measurements between.
// 	fBefore string
// 	fAfter  string
//
// 	// Reset and recompute the route tables if they don't exists.
// 	fForceTableReset bool
//
// 	// This is the number of workers which will be used in the computation.
// 	fNumWorkers int
//
// 	// This is the chunk size used to compute the route table.
// 	fChunkSize int
//
// 	// Where the output of the score computation would be saved, empty for stdout.
// 	fOutput string
// )
//
// // Returns the io.Writer to write the output to.
// func getOutput() (io.WriteCloser, error) {
// 	if fOutput == "" {
// 		return os.Stdout, nil
// 	}
// 	return os.Open(fOutput)
// }
//
// var ScoreCmd = &cobra.Command{
// 	Use:   "score",
// 	Short: "This command is used to compute the route score for the ip addresses.",
// 	Long:  "...",
// }
//
// func init() {
// 	ScoreCmd.AddCommand(ScoreIrisCmd)
// 	ScoreCmd.AddCommand(ScoreArkCmd)
//
// 	// Flags
// 	ScoreCmd.PersistentFlags().StringVar(&fBefore, "before", config.DefaultBeforeTimeFlag, "use this to retrieve data before this time")
// 	ScoreCmd.PersistentFlags().StringVar(&fAfter, "after", config.DefaultBeforeTimeFlag, "use this to retrieve data after this time")
// 	ScoreCmd.PersistentFlags().IntVar(&fNumWorkers, "num-workers", config.DefaultNumWorkers, "use this to denote number of workers")
// 	ScoreCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", config.DefaultChunkSize, "use this to limit the chunk size")
//
// 	// Connection relating flag
// 	ScoreCmd.PersistentFlags().String("iris-research-clickhouse-dsn", "", "use this to connect the Iris Clickhouse server")
//
// 	// Flags with a shorthand
// 	ScoreCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", config.DefaultForcedResetFlag, "use this to delete and recompute all of the tables")
// 	ScoreCmd.PersistentFlags().StringVarP(&fOutput, "output", "o", config.DefaultOutputFlag, "use this to determine the output file, empty for stdout")
//
// 	// Bind the dsn flag to environment variable
// 	viper.BindPFlag("iris-research-clickhouse-dsn", ScoreCmd.Flags().Lookup("iris-research-clickhouse-dsn"))
// 	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
// }
