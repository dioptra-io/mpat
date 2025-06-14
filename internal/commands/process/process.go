package process

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func ProcessCmd() *cobra.Command {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Process data from ClickHouse to ClickHouse.",
		Long:  "Stream the data from the given table, process it, and upload it into ClickHouse database.",
		Args:  cobra.ArbitraryArgs,
		Run:   processCmd,
	}

	processForwardingDecision := &cobra.Command{
		Use:   "forwarding-decision <input-table> <output-table>",
		Short: "Compute forwarding decision",
		Long:  "Compute the forwarding decision table given in forwarding info design doc.",
		Args:  cobra.ArbitraryArgs,
		Run:   processForwardingDecisionCmd,
	}
	processForwardingDecision.Flags().IntP("parallel-workers", "w", config.DefaultNumParallelWorkersInPipeline, "number of parallel workers spawned")
	viper.BindPFlag("parallel-workers", processForwardingDecision.Flags().Lookup("parallel-workers"))

	processCmd.AddCommand(processForwardingDecision)

	return processCmd
}

func processCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func processForwardingDecisionCmd(cmd *cobra.Command, args []string) {
}
