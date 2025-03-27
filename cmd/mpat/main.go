package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dioptra-io/ufuk-research/internal/commands/copy"
	"github.com/dioptra-io/ufuk-research/internal/commands/score"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var Version string = "unknown"

var logger = util.GetLogger()

var rootCmd = &cobra.Command{
	Use:     "mpat [module name]",
	Short:   "MPAT: Measurement Platform Analysis Tool",
	Long:    "",
	Version: Version,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Set the default arguments for logging
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
		debug := viper.GetBool("debug")

		if debug {
			util.SetLogLevel(util.LevelDebug)
		} else {
			util.SetLogLevel(util.LevelNormal)
		}

		logger.Debugln("RootCmd prerun succesfull.")
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Get binary version",
	Long:  "Get the version of the program",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("mpat version %s\n", Version)
	},
}

func init() {
	godotenv.Load(".env")

	rootCmd.AddCommand(versionCmd)

	// Add the silent and debug flag
	rootCmd.PersistentFlags().Bool("debug", false, "use this to see the debug messages")

	// Bind the variables to viper
	viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))

	// Configure env variables
	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		logger.Panicf("Error loading .env file: %s", err)
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		logger.Panicf("fatal error encountered while running the command: %s", err)
		os.Exit(1)
	}
}
