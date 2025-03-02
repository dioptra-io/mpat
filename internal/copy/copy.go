package util

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal/copy/arkdata"
	"dioptra-io/ufuk-research/internal/copy/irisdata"
)

var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "This command is ued for utility purposes mainly for copying data from different sources.",
	Long:  "...",
}

func init() {
	CopyCmd.AddCommand(irisdata.CopyIrisDataCmd)
	CopyCmd.AddCommand(arkdata.CopyArkDataCmd)
}
