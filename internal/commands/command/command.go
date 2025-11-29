package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

var (
	serverURL  string
	jsonOutput bool
	showAll    bool
	showTasks  bool
)

// -----------------------------------------------------------------------------
// ROOT COMMAND
// -----------------------------------------------------------------------------

func CommandCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "command",
		Short: "Manage the MPAT scheduler queue",
	}

	cmd.PersistentFlags().StringVar(&serverURL, "server", getServerURL(), "API server URL")

	cmd.AddCommand(psCmd())
	cmd.AddCommand(inspectCmd())
	// cmd.AddCommand(currentCmd())
	// cmd.AddCommand(pauseCmd())
	// cmd.AddCommand(requeueCmd())
	// cmd.AddCommand(priorityCmd())

	return cmd
}

func getServerURL() string {
	if s := os.Getenv("MPAT_SERVER_URL"); s != "" {
		return s
	}
	return "http://localhost:8080"
}

// -----------------------------------------------------------------------------
// HTTP HELPERS
// -----------------------------------------------------------------------------

func apiGET[T any](path string) (*T, error) {
	resp, err := http.Get(serverURL + path)
	if err != nil {
		return nil, fmt.Errorf("GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, body)
	}

	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &out, nil
}

func apiGETList[T any](path string) ([]T, error) {
	resp, err := http.Get(serverURL + path)
	if err != nil {
		return nil, fmt.Errorf("GET failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, body)
	}

	var out []T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}

func apiPOST(path string, body any) (*http.Response, error) {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	}
	return http.Post(serverURL+path, "application/json", buf)
}

func apiPUT(path string, body any) (*http.Response, error) {
	data, _ := json.Marshal(body)
	req, err := http.NewRequest("PUT", serverURL+path, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return (&http.Client{Timeout: 10 * time.Second}).Do(req)
}

// -----------------------------------------------------------------------------
// queue ps
// -----------------------------------------------------------------------------

func psCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "List commands in the queue",
		RunE:  psCmdRun,
	}

	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "Show all commands (not only active)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output JSON")
	return cmd
}

func psCmdRun(cmd *cobra.Command, args []string) error {
	commands, err := apiGETList[api.Command]("/api/v1/commands")
	if err != nil {
		return err
	}

	if !showAll {
		filtered := make([]api.Command, 0)
		for _, c := range commands {
			if c.Status == api.CommandStatusReady || c.Status == api.CommandStatusRunning {
				filtered = append(filtered, c)
			}
		}
		commands = filtered
	}

	if jsonOutput {
		b, _ := json.MarshalIndent(commands, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	displayCommandsTable(commands)
	return nil
}

func displayCommandsTable(commands []api.Command) {
	if len(commands) == 0 {
		fmt.Println("No commands.")
		return
	}

	// --- APPLY SORTING ---
	commands = sortCommands(commands)

	// Header
	fmt.Printf(
		"%-5s %-10s %-8s %-30s %-20s %-20s %-10s %-8s\n",
		"ID",
		"STATUS",
		"PRIO",
		"PAYLOAD",
		"CREATED",
		"FINISHED",
		"DURATION",
		"TASKS",
	)

	for _, c := range commands {
		// Count tasks
		done := 0
		for _, t := range c.Tasks {
			if t.Status == api.TaskStatusCompleted {
				done++
			}
		}
		total := len(c.Tasks)

		// Payload trimmed
		payload := c.Payload
		if len(payload) > 28 {
			payload = payload[:27] + "…"
		}

		// Created
		created := c.CreatedAt.Format("2006-01-02 15:04:05")

		// Finished
		finished := "-"
		if c.FinishedAt != nil {
			finished = c.FinishedAt.Format("2006-01-02 15:04:05")
		}

		// Duration
		duration := "-"
		if c.FinishedAt != nil {
			d := c.FinishedAt.Sub(c.CreatedAt)
			duration = humanDuration(d)
		}

		fmt.Printf(
			"%-5d %-10s %-8d %-30s %-20s %-20s %-10s %d/%d\n",
			c.ID,
			c.Status,
			c.Priority,
			payload,
			created,
			finished,
			duration,
			done,
			total,
		)
	}
}

func humanDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		min := int(d.Minutes())
		sec := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", min, sec)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", h, m)
}

func sortCommands(cmds []api.Command) []api.Command {
	unfinished := make([]api.Command, 0)
	finished := make([]api.Command, 0)

	for _, c := range cmds {
		if c.IsFinished() {
			finished = append(finished, c)
		} else {
			unfinished = append(unfinished, c)
		}
	}

	// Sort each group by CreatedAt ascending
	sort.Slice(unfinished, func(i, j int) bool {
		return unfinished[i].CreatedAt.Before(unfinished[j].CreatedAt)
	})

	sort.Slice(finished, func(i, j int) bool {
		return finished[i].CreatedAt.Before(finished[j].CreatedAt)
	})

	return append(unfinished, finished...)
}

// -----------------------------------------------------------------------------
// queue inspect
// -----------------------------------------------------------------------------

func inspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect <command-id>",
		Short: "Inspect a command",
		Args:  cobra.ExactArgs(1),
		RunE:  inspectCmdRun,
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output JSON")
	cmd.Flags().BoolVar(&showTasks, "tasks", false, "Show tasks")
	return cmd
}

func inspectCmdRun(cmd *cobra.Command, args []string) error {
	id64, _ := strconv.ParseUint(args[0], 10, 32)

	command, err := apiGET[api.Command](fmt.Sprintf("/api/v1/commands/%d", id64))
	if err != nil {
		return err
	}

	if jsonOutput {
		b, _ := json.MarshalIndent(command, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	displayCommandDetails(command, showTasks)
	return nil
}

func displayCommandDetails(c *api.Command, show bool) {
	fmt.Printf("Command %d\n", c.ID)
	fmt.Printf("Status:   %s\n", c.Status)
	fmt.Printf("Priority: %d\n", c.Priority)
	fmt.Printf("Created:  %s\n\n", c.CreatedAt.Format(time.RFC3339))

	done := 0
	for _, t := range c.Tasks {
		if t.Status == api.TaskStatusCompleted {
			done++
		}
	}
	fmt.Printf("Tasks: %d/%d completed\n\n", done, len(c.Tasks))

	for _, t := range c.Tasks {
		icon := map[api.TaskStatus]string{
			api.TaskStatusCompleted: "[✓]",
			api.TaskStatusRunning:   "[→]",
			api.TaskStatusFailed:    "[✗]",
			api.TaskStatusOrphaned:  "[!]",
			api.TaskStatusReady:     "[ ]",
		}[t.Status]

		fmt.Printf("%s %-20s %s\n", icon, string(t.NodeNV), t.Status)

		if show {
			fmt.Printf("  Created:  %s\n", t.CreatedAt.Format(time.RFC3339))
			if t.FinishedAt != nil {
				fmt.Printf("  Finished: %s\n", t.FinishedAt.Format(time.RFC3339))
				fmt.Printf("  Duration: %s\n", t.FinishedAt.Sub(t.CreatedAt))
			}
			fmt.Println()
		}
	}
}

// -----------------------------------------------------------------------------
// queue current
// -----------------------------------------------------------------------------

func currentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current",
		Short: "Show currently running command",
		RunE:  currentCmdRun,
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output JSON")
	cmd.Flags().BoolVar(&showTasks, "tasks", true, "Show tasks")
	return cmd
}

func currentCmdRun(cmd *cobra.Command, args []string) error {
	current, err := apiGET[api.Command]("/api/v1/commands/current")
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			fmt.Println("No command running.")
			return nil
		}
		return err
	}

	if jsonOutput {
		b, _ := json.MarshalIndent(current, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	displayCommandDetails(current, showTasks)
	return nil
}

// -----------------------------------------------------------------------------
// queue pause
// -----------------------------------------------------------------------------

func pauseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause <command-id>",
		Short: "Pause a running or ready command",
		Args:  cobra.ExactArgs(1),
		RunE:  pauseCmdRun,
	}
	return cmd
}

func pauseCmdRun(cmd *cobra.Command, args []string) error {
	id64, _ := strconv.ParseUint(args[0], 10, 32)

	resp, err := apiPOST(fmt.Sprintf("/api/v1/commands/%d/pause", id64), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%d: %s", resp.StatusCode, body)
	}

	fmt.Printf("Command %d paused.\n", id64)
	return nil
}

// -----------------------------------------------------------------------------
// queue requeue
// -----------------------------------------------------------------------------

func requeueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "requeue <command-id>",
		Short: "Requeue a sleeping command",
		Args:  cobra.ExactArgs(1),
		RunE:  requeueCmdRun,
	}
	return cmd
}

func requeueCmdRun(cmd *cobra.Command, args []string) error {
	id64, _ := strconv.ParseUint(args[0], 10, 32)

	resp, err := apiPOST(fmt.Sprintf("/api/v1/commands/%d/requeue", id64), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%d: %s", resp.StatusCode, body)
	}

	fmt.Printf("Command %d requeued.\n", id64)
	return nil
}

// -----------------------------------------------------------------------------
// queue priority
// -----------------------------------------------------------------------------

func priorityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "priority <command-id> <priority>",
		Short: "Update command priority",
		Args:  cobra.ExactArgs(2),
		RunE:  priorityCmdRun,
	}
	return cmd
}

func priorityCmdRun(cmd *cobra.Command, args []string) error {
	id64, _ := strconv.ParseUint(args[0], 10, 32)
	priority, _ := strconv.ParseUint(args[1], 10, 32)

	body := map[string]uint{"priority": uint(priority)}

	resp, err := apiPUT(fmt.Sprintf("/api/v1/commands/%d/priority", id64), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%d: %s", resp.StatusCode, data)
	}

	fmt.Printf("Priority updated: command %d → %d\n", id64, priority)
	return nil
}
