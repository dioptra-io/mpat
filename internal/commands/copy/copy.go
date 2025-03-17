package copy

import (
	"github.com/spf13/cobra"
)

// This is defined to put and pass values to the functions using context.Context.
// The key of the context as a string is not best practice as it can suffer from
// name colisions.
type contextKey string

var keyForceTableReset contextKey = "keyForceTableReset"

var (
	fBefore                    string
	fAfter                     string
	fIrisAPIUser               string
	fIrisAPIPassword           string
	fIrisAPIUrl                string
	fIrisResearchClickHouseDSN string

	fParallelDownloads int
	fChunkSize         int
	fForceTableReset   bool
)

var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "This command is ued for utility purposes mainly for copying data from different sources.",
	Long:  "...",
}

func init() {
	CopyCmd.AddCommand(CopyIrisCmd)
	CopyCmd.AddCommand(CopyArkCmd)
}
