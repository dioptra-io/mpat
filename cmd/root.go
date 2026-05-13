/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/

package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var (
	debug  bool
	logger *slog.Logger

	// injected at build time
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mp",
	Short: "MPAT command-line interface",
	Long: `MPAT (Measurement Platform Analysis Tool) is a platform
for collecting, managing, and analyzing large-scale traceroute
and internet measurement datasets.`,

	Version: version,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		level := slog.LevelInfo

		if debug {
			level = slog.LevelDebug
		}

		handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		})

		logger = slog.New(handler)
		slog.SetDefault(logger)

		logger.Debug(
			"debug logging enabled",
			"version", version,
			"commit", commit,
			"build_date", date,
		)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("command execution failed", "error", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVar(
		&debug,
		"debug",
		false,
		"enable debug logging",
	)

	rootCmd.SetVersionTemplate(
		fmt.Sprintf(
			"mp version %s\ncommit: %s\nbuilt: %s\n",
			version,
			commit,
			date,
		),
	)
}
