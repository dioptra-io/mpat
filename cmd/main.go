package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/iris"
)

var (
	fDatabase       string
	fUser           string
	fPassword       string
	fClickhouseHost string
)

var rootCmd = &cobra.Command{
	Use:   "mpat [module name]",
	Short: "Run the analysisctl",
	Long:  "",
}

func init() {
	rootCmd.AddCommand(iris.IrisCmd)

	// Set the persistent flags
	rootCmd.PersistentFlags().
		StringVarP(&fDatabase, "database", "d", "iris", "set the name of the database")
	rootCmd.PersistentFlags().StringVarP(&fUser, "user", "u", "default", "set the name of the user")
	rootCmd.PersistentFlags().
		StringVarP(&fPassword, "password", "p", "", "set the name of the password")
	rootCmd.PersistentFlags().
		StringVarP(&fClickhouseHost, "host", "a", "localhost:9000", "set the host address and the port of the clickhouse")

	// Bind the variables to viper
	viper.BindPFlag("database", rootCmd.PersistentFlags().Lookup("database"))
	viper.BindPFlag("user", rootCmd.PersistentFlags().Lookup("user"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
