package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/client"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get the tasks with applied filtering",
	RunE: func(cmd *cobra.Command, args []string) error {
		return getTasks(cmd.Context(), "")
	},
}

func newStatusCmd(name, status string) *cobra.Command {
	shortName := "List " + status + " tasks"
	if status == "" {
		shortName = "List all tasks"
	}
	return &cobra.Command{
		Use:   name,
		Short: shortName,
		RunE: func(cmd *cobra.Command, args []string) error {
			return getTasks(cmd.Context(), status)
		},
	}
}

func printTasks(tasks []api.Task) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "UUID\tSTATUS\tTYPE")

	for _, task := range tasks {

		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%s\n",
			task.UUID,
			task.Status,
			task.Type(),
		)
	}

	return nil
}

func getTasks(ctx context.Context, status string) error {
	client := client.NewClient(addr)

	tasks, err := client.ListTasks(ctx, status)
	if err != nil {
		return err
	}

	return printTasks(tasks)
}

func init() {
	rootCmd.AddCommand(getCmd)

	getCmd.AddCommand(
		newStatusCmd("queued", "queued"),
		newStatusCmd("running", "running"),
		newStatusCmd("done", "done"),
		newStatusCmd("failed", "failed"),
		newStatusCmd("canceled", "canceled"),
		newStatusCmd("terminamed", "terminated"),
		newStatusCmd("all", ""),
	)
}
