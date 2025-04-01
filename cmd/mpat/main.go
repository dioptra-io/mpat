package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dioptra-io/ufuk-research/internal/compute"
	"github.com/dioptra-io/ufuk-research/internal/cp"
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
			logger.SetFormatter(&logrus.TextFormatter{
				DisableColors: false,
				FullTimestamp: true,
				// ForceQuote:    true,
			})
			// logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
			logger.SetOutput(os.Stderr)

			if debug {
				util.SetLogLevel(util.LevelDebug)
			} else {
				util.SetLogLevel(util.LevelNormal)
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
	rootCmd.AddCommand(cp.CpCmd())
	rootCmd.AddCommand(compute.ComputeCmd())

	// Add the silent and debug flag
	rootCmd.PersistentFlags().Bool("debug", false, "see debug messages")

	// Bind flag to viper
	viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))

	// Bind the variables to viper
	viper.BindEnv("debug", "MPAT_DEBUG")

	viper.AutomaticEnv() // read env variables for binding to flags

	// run root command
	if err := rootCmd.Execute(); err != nil {
		logger.Panicf("fatal error encountered while running the command: %s", err)
		os.Exit(1)
	}
}
