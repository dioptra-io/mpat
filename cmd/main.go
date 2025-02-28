package main

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/copy"
	"dioptra-io/ufuk-research/internal/iris"
	"dioptra-io/ufuk-research/internal/log"
)

var (
	fDatabase       string
	fUser           string
	fPassword       string
	fClickhouseHost string
	fSilent         bool
	fDebug          bool
)

var logger = log.GetLogger()

var rootCmd = &cobra.Command{
	Use:   "mpat [module name]",
	Short: "MPAT: Measurement Platform Analysis Tool",
	Long:  "",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Set the default arguments for logging
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
		if fSilent {
			log.SetLogLevel(log.LevelSilent)
		} else {
			if fDebug {
				log.SetLogLevel(log.LevelDebug)
			} else {
				log.SetLogLevel(log.LevelNormal)
			}
		}

		logger.Debugln("RootCmd prerun succesfull.")
	},
}

func init() {
	godotenv.Load(".env")

	rootCmd.AddCommand(iris.IrisCmd)
	rootCmd.AddCommand(util.CopyCmd)

	// Set the persistent flags
	rootCmd.PersistentFlags().
		StringVarP(&fDatabase, "database", "d", "iris", "set the name of the database")
	rootCmd.PersistentFlags().StringVarP(&fUser, "user", "u", "default", "set the name of the user")
	rootCmd.PersistentFlags().
		StringVarP(&fPassword, "password", "p", "", "set the name of the password")
	rootCmd.PersistentFlags().
		StringVarP(&fClickhouseHost, "host", "a", "localhost:9000", "set the host address and the port of the clickhouse")
	rootCmd.PersistentFlags().
		BoolVarP(&fSilent, "silent", "s", false, "set this flag to suppress logs")
	rootCmd.PersistentFlags().
		BoolVar(&fDebug, "debug", false, "set this flag to see debug messages, if silent is set then this flag is ignored")

	// Bind the variables to viper
	viper.BindPFlag("database", rootCmd.PersistentFlags().Lookup("database"))
	viper.BindPFlag("user", rootCmd.PersistentFlags().Lookup("user"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("slent", rootCmd.PersistentFlags().Lookup("silent"))
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
