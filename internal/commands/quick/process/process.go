package process

import (
	"github.com/spf13/cobra"

	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func ProcessCmd() *cobra.Command {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Process jsonl to stderr",
		Long:  "Process a given type of object. Read stdin write stdout",
		Args:  processCmdArgs,
		Run:   processCmd,
	}

	return processCmd
}

func processCmdArgs(cmd *cobra.Command, args []string) error {
	return nil
}

func processCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}
