package iris

import (
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal/iris/score"
)

var IrisCmd = &cobra.Command{
	Use:   "iris",
	Short: "This is the command module used with Iris operations.",
	Long:  "This is the command module for Iris. Iris is an open source measurement platform developed at Sorbonne University. For more information refer to the https://dioptra.io",
}

func init() {
	IrisCmd.AddCommand(score.IrisScoreCmd)
}
