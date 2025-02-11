package routes

import (
	"fmt"

	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal/routes/compute"
	"dioptra-io/ufuk-research/internal/routes/score"
)

var RoutesCmd = &cobra.Command{
	Use:   "routes",
	Short: "This is the command relating to the routes table",
	Long:  "This command is used to perform operations related to routes and route tables. For more information check the design doc.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Available commands:")
		fmt.Println("   compute")
	},
}

func init() {
	RoutesCmd.AddCommand(compute.RoutesComputeCmd)
	RoutesCmd.AddCommand(score.RoutesScoreCmd)
}
