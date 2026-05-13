package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/spf13/cobra"
	"sort"
	"strings"
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

func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func printTasks(tasks []api.Task) error {
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Created.After(tasks[j].Created)
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "UUID\tSTATUS\tAGE\tTYPE\tARGS")

	for _, task := range tasks {
		args := task.Args()

		argParts := make([]string, 0, len(args))
		for k, v := range args {
			argParts = append(argParts, fmt.Sprintf("%s=%s", k, v))
		}

		sort.Strings(argParts)

		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\n",
			task.UUID,
			task.Status,
			humanDuration(time.Since(task.Created)),
			task.Type(),
			strings.Join(argParts, ", "),
		)
	}

	return nil
}

func getTasks(ctx context.Context, status string) error {
	client := mpat.NewClient(addr)

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
