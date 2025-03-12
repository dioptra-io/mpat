package copycmd

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal/log"
)

var logger = log.GetLogger()

var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "This command is ued for utility purposes mainly for copying data from different sources.",
	Long:  "...",
}

func init() {
	CopyCmd.AddCommand(CopyIrisCmd)
	// CopyCmd.AddCommand(irisdata.CopyIrisDataCmd)
	// CopyCmd.AddCommand(arkdata.CopyArkDataCmd)
}
