package cmd

import (
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/spf13/cobra"
)

var cancelCmd = &cobra.Command{
	Use:   "cancel <task-uuid>",
	Short: "Cancel a task",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		taskUUID := args[0]

		client := mpat.NewClient(addr)

		if err := client.CancelTask(cmd.Context(), taskUUID); err != nil {
			return err
		}

		fmt.Println(taskUUID)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(cancelCmd)
}
