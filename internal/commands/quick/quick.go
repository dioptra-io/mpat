package quick

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dioptra-io/ufuk-research/internal/commands/quick/stream"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func QuickCmd() *cobra.Command {
	quickCmd := &cobra.Command{
		Use:   "quick",
		Short: "Quick input quick output services",
		Long:  "This module provides quick i/o services",
		Args:  quickCmdArgs,
		Run:   quickCmd,
	}
	quickCmd.PersistentFlags().StringP("research-dsn", "s", "", "research clickhouse dsn string")
	quickCmd.PersistentFlags().StringP("production-dsn", "p", "", "production clickhouse dsn string")

	viper.BindPFlag("research_dsn", quickCmd.PersistentFlags().Lookup("research-dsn"))
	viper.BindPFlag("production_dsn", quickCmd.PersistentFlags().Lookup("production-dsn"))

	viper.SetEnvPrefix("QUICK")
	viper.AutomaticEnv()

	quickCmd.AddCommand(stream.StreamCmd())

	return quickCmd
}

func quickCmdArgs(cmd *cobra.Command, args []string) error {
	return nil
}

func quickCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}
