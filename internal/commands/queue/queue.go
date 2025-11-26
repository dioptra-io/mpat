package queue

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/api"
	"github.com/spf13/cobra"
)

var (
	serverURL  string
	showAll    bool
	jsonOutput bool
	showTasks  bool
)

// QueueCmd returns the queue command
func QueueCmd() *cobra.Command {
	queueCmd := &cobra.Command{
		Use:   "queue",
		Short: "Manage command queue",
		Long:  "Manage and monitor the MPAT command queue",
	}

	// Add persistent flags
	queueCmd.PersistentFlags().StringVar(&serverURL, "server", getServerURL(), "API server URL")

	// Add subcommands
	queueCmd.AddCommand(psCmd())
	queueCmd.AddCommand(inspectCmd())
	queueCmd.AddCommand(currentCmd())
	queueCmd.AddCommand(stopCmd())
	queueCmd.AddCommand(resumeCmd())
	queueCmd.AddCommand(priorityCmd())

	return queueCmd
}

// getServerURL returns the server URL from env or default
func getServerURL() string {
	if url := os.Getenv("MPAT_SERVER_URL"); url != "" {
		return url
	}
	return "http://localhost:8080"
}

// psCmd returns the ps subcommand
func psCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "List commands",
		Long:  "List all active commands in the queue",
		RunE:  psCmdRun,
	}

	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "Show all commands (including inactive)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	return cmd
}

func psCmdRun(cmd *cobra.Command, args []string) error {
	// Make API request
	resp, err := http.Get(serverURL + "/api/v1/commands")
	if err != nil {
		return fmt.Errorf("failed to fetch commands: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var commands []api.Command
	if err := json.NewDecoder(resp.Body).Decode(&commands); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Filter by active if not showing all
	if !showAll {
		filtered := make([]api.Command, 0)
		for _, cmd := range commands {
			if cmd.Active {
				filtered = append(filtered, cmd)
			}
		}
		commands = filtered
	}

	// Output
	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(commands)
	}

	// Table output
	displayCommandsTable(commands)
	return nil
}

func displayCommandsTable(commands []api.Command) {
	if len(commands) == 0 {
		fmt.Println("No commands found")
		return
	}

	// Print header
	fmt.Printf("%-5s %-10s %-10s %-20s %-10s\n", "ID", "PRIORITY", "STATUS", "CREATED AT", "TASKS")
	fmt.Println(strings.Repeat("-", 70))

	for _, cmd := range commands {
		status := "Inactive"
		if cmd.Active {
			status = "Active"
		}

		// Count tasks
		completed := 0
		total := len(cmd.Tasks)
		for _, task := range cmd.Tasks {
			if task.Status == api.StatusDone {
				completed++
			}
		}

		tasksStr := fmt.Sprintf("%d/%d", completed, total)

		fmt.Printf("%-5d %-10d %-10s %-20s %-10s\n",
			cmd.ID,
			cmd.Priority,
			status,
			cmd.CreatedAt.Format("2006-01-02 15:04:05"),
			tasksStr,
		)
	}
}

// inspectCmd returns the inspect subcommand
func inspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect <command-id>",
		Short: "Inspect a command",
		Long:  "Show detailed information about a specific command",
		Args:  cobra.ExactArgs(1),
		RunE:  inspectCmdRun,
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().BoolVar(&showTasks, "tasks", false, "Show full task details")

	return cmd
}

func inspectCmdRun(cmd *cobra.Command, args []string) error {
	commandID, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid command id: %w", err)
	}

	// Make API request
	url := fmt.Sprintf("%s/api/v1/commands/%d", serverURL, commandID)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var command api.Command
	if err := json.NewDecoder(resp.Body).Decode(&command); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Output
	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(command)
	}

	// Display command details
	displayCommandDetails(&command, showTasks)
	return nil
}

func displayCommandDetails(cmd *api.Command, showTaskDetails bool) {
	status := "Inactive"
	if cmd.Active {
		status = "Active"
	}

	fmt.Printf("Command ID: %d\n", cmd.ID)
	fmt.Printf("Priority:   %d\n", cmd.Priority)
	fmt.Printf("Status:     %s\n", status)
	fmt.Printf("Created:    %s\n", cmd.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Params:     %s\n", cmd.Params)
	fmt.Println()

	// Count completed tasks
	completed := 0
	for _, task := range cmd.Tasks {
		if task.Status == api.StatusDone {
			completed++
		}
	}

	fmt.Printf("Tasks (%d/%d completed):\n", completed, len(cmd.Tasks))
	fmt.Println()

	// Print table header
	if showTaskDetails {
		fmt.Printf("  %-5s %-25s %-12s %-20s %-20s %-15s\n",
			"ID", "NODE", "STATUS", "CREATED", "FINISHED", "DURATION")
		fmt.Printf("  %s\n", strings.Repeat("-", 105))
	}

	for _, task := range cmd.Tasks {
		var statusIcon string
		switch task.Status {
		case api.StatusDone:
			statusIcon = "[✓]"
		case api.StatusRunning:
			statusIcon = "[→]"
		case api.StatusFailed:
			statusIcon = "[✗]"
		case api.StatusWaiting:
			statusIcon = "[⏸]"
		default:
			statusIcon = "[ ]"
		}

		nodeName := fmt.Sprintf("%s v%d", task.NodeNamedVersion.Name, task.NodeNamedVersion.Version)

		if showTaskDetails {
			// Calculate duration
			durationStr := "-"
			if !task.FinishedAt.IsZero() && task.FinishedAt.After(task.CreatedAt) {
				duration := task.FinishedAt.Sub(task.CreatedAt)
				durationStr = formatDuration(duration)
			}

			finishTimeStr := "-"
			if !task.FinishedAt.IsZero() {
				finishTimeStr = task.FinishedAt.Format("15:04:05")
			}

			orphanMark := ""
			if task.Orphan {
				orphanMark = " [ORPHAN]"
			}

			fmt.Printf("  %-5d %-25s %-12s %-20s %-20s %-15s%s\n",
				task.ID,
				nodeName,
				string(task.Status),
				task.CreatedAt.Format("15:04:05"),
				finishTimeStr,
				durationStr,
				orphanMark,
			)
		} else {
			orphanStr := ""
			if task.Orphan {
				orphanStr = " [ORPHAN]"
			}
			fmt.Printf("  %s %-25s - %s%s\n", statusIcon, nodeName, task.Status, orphanStr)
		}
	}
}

// formatDuration formats a duration in human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", hours, minutes)
}

// currentCmd returns the current subcommand
func currentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current",
		Short: "Show current running command",
		Long:  "Show the currently running command (if any)",
		RunE:  currentCmdRun,
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().BoolVar(&showTasks, "tasks", true, "Show full task details")

	return cmd
}

func currentCmdRun(cmd *cobra.Command, args []string) error {
	// Make API request
	url := fmt.Sprintf("%s/api/v1/commands/current", serverURL)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch current command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("No command is currently running")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var command api.Command
	if err := json.NewDecoder(resp.Body).Decode(&command); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Output
	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(command)
	}

	displayCommandDetails(&command, showTasks)
	return nil
}

// stopCmd returns the stop subcommand
func stopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop currently running command",
		Long:  "Stop the currently executing command/task",
		RunE:  stopCmdRun,
	}

	return cmd
}

func stopCmdRun(cmd *cobra.Command, args []string) error {
	// Get current command first
	url := fmt.Sprintf("%s/api/v1/commands/current", serverURL)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch current command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("No command is currently running")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var command api.Command
	if err := json.NewDecoder(resp.Body).Decode(&command); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Confirm
	fmt.Printf("Stop command %d? (y/N): ", command.ID)
	var confirm string
	fmt.Scanln(&confirm)
	if confirm != "y" && confirm != "Y" {
		fmt.Println("Cancelled")
		return nil
	}

	// Make DELETE request
	deleteURL := fmt.Sprintf("%s/api/v1/commands/%d", serverURL, command.ID)
	req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to stop command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Command %d stopped\n", command.ID)
	return nil
}

// resumeCmd returns the resume subcommand
func resumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume <command-id>",
		Short: "Resume a command",
		Long:  "Requeue a command to resume execution",
		Args:  cobra.ExactArgs(1),
		RunE:  resumeCmdRun,
	}

	return cmd
}

func resumeCmdRun(cmd *cobra.Command, args []string) error {
	commandID, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid command id: %w", err)
	}

	// Make POST request
	url := fmt.Sprintf("%s/api/v1/commands/%d/requeue", serverURL, commandID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to resume command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var command api.Command
	if err := json.NewDecoder(resp.Body).Decode(&command); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Printf("Command %d resumed\n", commandID)
	displayCommandDetails(&command, false)
	return nil
}

// priorityCmd returns the priority subcommand
func priorityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "priority <command-id> <priority>",
		Short: "Set command priority",
		Long:  "Update the priority of a command",
		Args:  cobra.ExactArgs(2),
		RunE:  priorityCmdRun,
	}

	return cmd
}

func priorityCmdRun(cmd *cobra.Command, args []string) error {
	commandID, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid command id: %w", err)
	}

	priority, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid priority: %w", err)
	}

	// Make PUT request
	url := fmt.Sprintf("%s/api/v1/commands/%d/priority", serverURL, commandID)
	reqBody := fmt.Sprintf(`{"priority": %d}`, priority)

	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to set priority: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Command %d priority set to %d\n", commandID, priority)
	return nil
}
