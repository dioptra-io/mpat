package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/nexthop"
)

var (
	fDatabase       string
	fUser           string
	fPassword       string
	fClickhouseHost string
)

var rootCmd = &cobra.Command{
	Use:   "consumercli [module name]",
	Short: "Runs the consumer module",
	Long:  "This is a program that consumes the data in the clikhouse database and computes a result.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Please provide the a module to run.")
		os.Exit(0)
	},
}

func init() {
	rootCmd.AddCommand(nexthop.CountNextHopsCmd)

	// Set the persistent flags
	rootCmd.PersistentFlags().
		StringVarP(&fDatabase, "database", "d", "iris", "set the name of the database")
	rootCmd.PersistentFlags().StringVarP(&fUser, "user", "u", "admin", "set the name of the user")
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
