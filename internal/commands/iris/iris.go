package iris

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dioptra-io/ufuk-research/internal/clients/iris"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var logger = log.GetLogger()

func init() {
	// Set default values
	viper.SetDefault("iris.base_url", "https://api.iris.dioptra.io")

	// Bind specific environment variables
	viper.BindEnv("iris.username", "MPAT_IRIS_USERNAME")
	viper.BindEnv("iris.password", "MPAT_IRIS_PASSWORD")
}

func IrisCmd() *cobra.Command {
	irisCmd := &cobra.Command{
		Use:   "iris",
		Short: "Iris related commands",
		Long:  "Iris and iris related commands",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	irisCmd.AddCommand(queryCmd())

	return irisCmd
}

func queryCmd() *cobra.Command {
	queryCmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Execute a SQL query on ClickHouse",
		Long:  "Execute a SQL query on ClickHouse and stream the results to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := args[0]

			// Set silent to true if not already set
			if !cmd.Flags().Changed("silent") {
				viper.Set("silent", true)
				logger.SetLevel(logrus.FatalLevel)
			}

			// Get credentials from viper/config
			baseURL := viper.GetString("iris.base_url")
			username := viper.GetString("iris.username")
			password := viper.GetString("iris.password")

			if baseURL == "" || username == "" || password == "" {
				return fmt.Errorf("iris credentials not configured (base_url, username, password)")
			}

			// Create Iris client
			client := iris.NewIrisClient(baseURL, username, password)

			ctx := context.Background()

			// Login
			logger.Info("Logging in to Iris...")
			if err := client.Login(ctx); err != nil {
				return fmt.Errorf("login failed: %w", err)
			}
			logger.Info("Login successful")

			// Execute query
			logger.Infof("Executing query: %s", query)
			reader, err := client.QueryClickHouse(ctx, query)
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

	return queryCmd
}
