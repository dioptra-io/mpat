package main

import (
	"fmt"
	"os"

	"github.com/dioptra-io/ufuk-research/internal/commands/ch"
	"github.com/dioptra-io/ufuk-research/internal/commands/command"
	"github.com/dioptra-io/ufuk-research/internal/commands/iris"
	"github.com/dioptra-io/ufuk-research/internal/commands/serve"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	Version   = "unknown"
	GitCommit = "unknown"
	BuildDate = "unknown"
	GoVersion = "unknown"
)

var logger = log.GetLogger()

func rootCmdPreRun(cmd *cobra.Command, args []string) {
	debug := viper.GetBool("debug")
	silent := viper.GetBool("silent")
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	logger.SetOutput(os.Stderr)

	if debug && silent {
		logger.Warn("Both silent and debug flags are set, silent flag will take precentence and program will only output essential messages.")
	}

	if debug {
		log.SetLogLevel(log.LevelDebug)
	} else {
		log.SetLogLevel(log.LevelNormal)
	}

	if silent {
		log.SetLogLevel(log.LevelSilent)
	}

	logger.Debugln("RootCmd prerun succesfull.")
}

func rootCmdRun(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func versionCmdRun(cmd *cobra.Command, args []string) {
	fmt.Printf("mpat version: %s\n", Version)
	fmt.Printf("git commit: %s\n", GitCommit)
	fmt.Printf("build date: %s\n", BuildDate)
	fmt.Printf("go version: %s\n", GoVersion)
}

func main() {
	rootCmd := &cobra.Command{
		Use:              "mpat",
		Short:            "MPAT: Measurement Platform Analysis Tool",
		Long:             "",
		Version:          Version,
		PersistentPreRun: rootCmdPreRun,
		Run:              rootCmdRun,
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Get binary and build versions",
		Long:  "Get the version of the program and build environment",
		Run:   versionCmdRun,
	}

	// Add commands there
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(serve.ServeCmd())
	rootCmd.AddCommand(command.CommandCmd())
	rootCmd.AddCommand(iris.IrisCmd())
	rootCmd.AddCommand(ch.CHCmd())

	// Add the silent and debug flag
	rootCmd.PersistentFlags().Bool("debug", false, "see debug messages")
	rootCmd.PersistentFlags().Bool("silent", false, "hide non-essential output")

	// Bind flag to viper
	viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("silent", rootCmd.PersistentFlags().Lookup("silent"))

	// Set other variables
	viper.Set("version", Version)
	viper.Set("gitcommit", GitCommit)
	viper.Set("builddate", BuildDate)
	viper.Set("goversion", GoVersion)

	// Bind environment variables to some flags
	viper.BindEnv("dsn", "MPAT_DSN")

	// read env variables for binding to flags
	viper.AutomaticEnv()

	// run root command
	if err := rootCmd.Execute(); err != nil {
		logger.Panicf("fatal error encountered while running the command: %s", err)
		os.Exit(1)
	}
}
