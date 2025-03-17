package main

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/cmd/copycmd"
	"dioptra-io/ufuk-research/cmd/scorecmd"
	"dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

var rootCmd = &cobra.Command{
	Use:   "mpat [module name]",
	Short: "MPAT: Measurement Platform Analysis Tool",
	Long:  "",
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

func init() {
	godotenv.Load(".env")

	rootCmd.AddCommand(scorecmd.ScoreCmd)
	rootCmd.AddCommand(copycmd.CopyCmd)

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
