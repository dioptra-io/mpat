package iriscmd

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/cmd/iriscmd/irisscorecmd"
)

var IrisCmd = &cobra.Command{
	Use:   "iris <table-names...>",
	Short: "This command is used to copy the Iris data.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	IrisCmd.AddCommand(irisscorecmd.IrisScoreCmd)
}
