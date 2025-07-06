package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dioptra-io/ufuk-research/internal/commands/metrics"
	"github.com/dioptra-io/ufuk-research/internal/commands/process"
	"github.com/dioptra-io/ufuk-research/internal/commands/upload"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var (
	Version   = "unknown"
	GitCommit = "unknown"
	BuildDate = "unknown"
	GoVersion = "unknown"
)

func main() {
	logger := util.GetLogger()

	rootCmd := &cobra.Command{
		Use:     "mpat",
		Short:   "MPAT: Measurement Platform Analysis Tool",
		Long:    "",
		Version: Version,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			debug := viper.GetBool("debug")
			silent := viper.GetBool("silent")
			logger.SetFormatter(&logrus.TextFormatter{
				DisableColors: false,
				FullTimestamp: true,
			})
			logger.SetOutput(os.Stderr)

			if debug {
				util.SetLogLevel(util.LevelDebug)
			} else {
				util.SetLogLevel(util.LevelNormal)
			}

			if silent {
				util.SetLogLevel(util.LevelSilent)
			}

			logger.Debugln("RootCmd prerun succesfull.")
		},
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Get binary version",
		Long:  "Get the version of the program",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("mpat version: %s\n", Version)
			fmt.Printf("git commit: %s\n", GitCommit)
			fmt.Printf("build date: %s\n", BuildDate)
			fmt.Printf("go version: %s\n", GoVersion)
		},
	}

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(upload.UpoadCmd())
	rootCmd.AddCommand(process.ProcessCmd())
	rootCmd.AddCommand(metrics.MetricsCmd())

	// Add the silent and debug flag
	rootCmd.PersistentFlags().Bool("debug", false, "see debug messages")
	rootCmd.PersistentFlags().Bool("silent", false, "hide non-functional output")
	rootCmd.PersistentFlags().Bool("force", false, "force delete the existing table")
	rootCmd.PersistentFlags().StringP("dsn", "r", "", "dsn string of research ClickHouse database")

	// Bind flag to viper
	viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("silent", rootCmd.PersistentFlags().Lookup("silent"))
	viper.BindPFlag("dsn", rootCmd.PersistentFlags().Lookup("dsn"))
	viper.BindPFlag("force", rootCmd.PersistentFlags().Lookup("force"))

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
