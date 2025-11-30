package ch

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dioptra-io/ufuk-research/internal/clients/clickhouse"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var logger = log.GetLogger()

func init() {
	// Bind specific environment variables
	viper.BindEnv("ch.dsn", "MPAT_CH_DSN")
}

func CHCmd() *cobra.Command {
	chCmd := &cobra.Command{
		Use:   "ch",
		Short: "ClickHouse related commands",
		Long:  "ClickHouse database related commands",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	chCmd.AddCommand(queryCmd())

	return chCmd
}

func queryCmd() *cobra.Command {
	var stdinFlag bool

	queryCmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Execute a SQL query on ClickHouse",
		Long:  "Execute a SQL query on ClickHouse and stream the results to stdout",
		Args: func(cmd *cobra.Command, args []string) error {
			// If --stdin is set, no args required
			if stdinFlag {
				return nil
			}
			// Otherwise, require exactly 1 arg
			return cobra.ExactArgs(1)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Set silent to true if not explicitly set by user
			if !cmd.Root().PersistentFlags().Changed("silent") {
				viper.Set("silent", true)
				logger.SetLevel(logrus.FatalLevel)
			}

			// Get DSN from viper/config
			dsn := viper.GetString("ch.dsn")
			if dsn == "" {
				return fmt.Errorf("ClickHouse DSN not configured (set MPAT_CH_DSN)")
			}

			// Parse DSN to extract connection details
			// Assuming DSN format: clickhouse://username:password@host:port/database
			// You'll need to parse this or use a different constructor
			// For now, let's assume you have a NewClickHouseClientFromDSN function
			client, err := clickhouse.NewClickHouseClientFromDSN(dsn)
			if err != nil {
				return fmt.Errorf("failed to create ClickHouse client: %w", err)
			}

			ctx := context.Background()

			// Ping to verify connection
			logger.Info("Connecting to ClickHouse...")
			if err := client.Ping(ctx); err != nil {
				return fmt.Errorf("failed to connect to ClickHouse: %w", err)
			}
			logger.Info("Connection successful")

			// Get query from stdin or args
			query := args[0]

			// Execute query and stream to stdout
			logger.Infof("Executing query: %s", query)
			reader, err := client.QueryStream(ctx, query)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			defer reader.Close()

			// Stream to stdout
			_, err = io.Copy(os.Stdout, reader)
			if err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}

			logger.Info("Query completed successfully")

			return nil
		},
	}

	queryCmd.Flags().BoolVar(&stdinFlag, "stdin", false, "Read query from stdin instead of arguments")

	return queryCmd
}
