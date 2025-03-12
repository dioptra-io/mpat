package copycmd

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/cmd/copycmd/copyarkcmd"
	"dioptra-io/ufuk-research/cmd/copycmd/copyiriscmd"
	"dioptra-io/ufuk-research/pkg/log"
)

var logger = log.GetLogger()

var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "This command is ued for utility purposes mainly for copying data from different sources.",
	Long:  "...",
}

func init() {
	CopyCmd.AddCommand(copyiriscmd.CopyIrisCmd)
	CopyCmd.AddCommand(copyarkcmd.CopyArkCmd)
}
