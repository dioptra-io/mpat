package scorecmd

import (
	"github.com/spf13/cobra"

	scoreiriscmd "dioptra-io/ufuk-research/cmd/scorecmd/irisscorecmd"
)

var ScoreCmd = &cobra.Command{
	Use:   "score",
	Short: "This command is used to compute the route score for the ip addresses.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	ScoreCmd.AddCommand(scoreiriscmd.ScoreIrisCmd)
}
