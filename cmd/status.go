package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/client"
	"github.com/spf13/cobra"
)

func newStatusCmd(name, status string) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: "List " + status + " tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := client.NewClient(addr)

			tasks, err := client.ListTasks(cmd.Context(), status)
			if err != nil {
				return err
			}

			return printTasks(tasks)
		},
	}
}

func printTasks(tasks []api.Task) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "UUID\tSTATUS\tARTIFACTS")

	for _, task := range tasks {
		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%d\n",
			task.UUID,
			task.Status,
			len(task.Artifacts),
		)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(
		newStatusCmd("queue", "queued"),
		newStatusCmd("running", "running"),
		newStatusCmd("done", "done"),
		newStatusCmd("failed", "failed"),
		newStatusCmd("canceled", "canceled"),
		newStatusCmd("terminated", "terminated"),
		newStatusCmd("ls", ""),
	)
}
