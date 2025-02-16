package util

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal/util/copyiristables"
)

var UtilCmd = &cobra.Command{
	Use:   "util",
	Short: "This command is ued for utility purposes.",
	Long:  "...",
}

func init() {
	UtilCmd.AddCommand(copyiristables.UtilCopyIrisTablesCmd)
}
