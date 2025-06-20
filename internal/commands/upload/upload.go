package upload

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	v1 "github.com/dioptra-io/ufuk-research/api/v1"
	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/internal/pipeline"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func UpoadCmd() *cobra.Command {
	uploadCmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload data to ClickHouse from a data source.",
		Long:  "Stream the data from the given source and upload it into ClickHouse database.",
		Args:  cobra.ArbitraryArgs,
		Run:   uploadCmd,
	}

	uploadIrisResultsCmd := &cobra.Command{
		Use:   "iris-results <source-date> <destination-table-name>",
		Short: "Copy the iris results table.",
		Long:  "Copy the iris results table from prod to working ClickHouse database.",
		Args:  cobra.ArbitraryArgs,
		Run:   uploadIrisResultsCmd,
	}
	uploadIrisResultsCmd.Flags().StringP("source-dsn", "s", "", "dsn string of production clickhouse database")
	uploadIrisResultsCmd.Flags().Bool("ipv4", true, "use only ipv4 measurement tables")
	viper.BindPFlag("source-dsn", uploadIrisResultsCmd.Flags().Lookup("source-dsn"))
	viper.BindPFlag("ipv4", uploadIrisResultsCmd.Flags().Lookup("ipv4"))
	viper.BindEnv("source-dsn", "MPAT_PROD_DSN")

	uploadArkResultsCmd := &cobra.Command{
		Use:   "ark-results <source-date> <destination-table-name>",
		Short: "Copy the ark results table.",
		Long:  "Copy the ark results table from prod to working ClickHouse database.",
		Args:  cobra.ExactArgs(2),
		Run:   uploadArkResultsCmd,
	}
	uploadArkResultsCmd.Flags().String("ark-user", "", "username for the ark database")
	uploadArkResultsCmd.Flags().String("ark-password", "", "password for the ark database")
	viper.BindPFlag("ark-user", uploadArkResultsCmd.Flags().Lookup("ark-user"))
	viper.BindPFlag("ark-password", uploadArkResultsCmd.Flags().Lookup("ark-password"))
	viper.BindEnv("ark-user", "MPAT_ARK_API_USER")
	viper.BindEnv("ark-password", "MPAT_ARK_API_PASSWORD")

	uploadCmd.AddCommand(uploadArkResultsCmd)
	uploadCmd.AddCommand(uploadIrisResultsCmd)

	return uploadCmd
}

func uploadCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func uploadIrisResultsCmd(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		logger.Printf("Upload command requires at least 2 arguments, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	destinationDSNString := viper.GetString("dsn")
	sourceDSNString := viper.GetString("source-dsn")
	onlyIPv4Measurements := viper.GetBool("ipv4")
	destinationTableName := args[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceDate, err := v1.ParseArkDate(args[0])
	if err != nil {
		logger.Errorf("Cannot parse given date: %v\n.", err)
		return
	}

	destinationNativeClient, err := clientv3.NewNativeSQLClientWithPing(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	destinationHTTPClient, err := clientv3.NewHTTPSQLClient(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database connection failed: %v.\n", err)
		return
	}

	sourceHTTPClient, err := clientv3.NewHTTPSQLClient(sourceDSNString)
	if err != nil {
		logger.Errorf("Source ClickHouse database connection failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	if tableSize, err := destinationNativeClient.TableSize(ctx, destinationTableName); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty table exists in the destination, try --force flag.")
		return
	}

	irisClient, err := clientv3.NewIrisClientWithJWT()
	if err != nil {
		logger.Errorf("Iris client health check failed: %v\n.", err)
		return
	}

	logger.Println("Iris API health check positive.")

	sourceTableNames, err := irisClient.GetTableNamesFor(onlyIPv4Measurements, false, sourceDate)
	if err != nil {
		logger.Errorf("Iris client fetch table failed: %v.\n", err)
		return
	}

	logger.Debugf("Running command with %d source tables.\n", len(sourceTableNames))

	destinationManager := pipeline.NewClickHouseManager[apiv3.IrisResultsRow](ctx, destinationNativeClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       destinationTableName,
		AddCheckIfExist: true,
		Database:        destinationNativeClient.Database,
	}, &queries.BasicCreateQuery{
		TableName:           destinationTableName,
		AddCheckIfNotExists: true,
		Database:            destinationNativeClient.Database,
		Object:              apiv3.IrisResultsRow{},
	})
	if err != nil {
		logger.Errorf("Destination ClickHouse database table reset failed: %v.\n", err)
		return
	}

	for i, tableName := range sourceTableNames {
		sourceStreamer := pipeline.NewClickHouseReaderStreamer(ctx, sourceHTTPClient)
		reader, err := sourceStreamer.Ingest(&queries.BasicSelectStartQuery{
			TableNames: []string{tableName}, // quick fix
			Database:   sourceHTTPClient.Database,
		})
		if err != nil {
			logger.Errorf("Failed to call ingress: %v.\n", err)
			return
		}

		destinationStreamer := pipeline.NewClickHouseReaderStreamer(ctx, destinationHTTPClient)
		err = destinationStreamer.Egress(reader, &queries.BasicInsertStartQuery{
			TableName: destinationTableName,
			Database:  destinationHTTPClient.Database,
		})
		if err != nil {
			logger.Errorf("Failed to call ingress: %v.\n", err)
			return
		}

		logger.Println("Started streaming.")

		var topGroup errgroup.Group
		topGroup.Go(sourceStreamer.G.Wait)
		topGroup.Go(destinationStreamer.G.Wait)

		if err := topGroup.Wait(); err != nil {
			logger.Errorf("An error occured while transfering data %v.\n", err)
			return
		}

		logger.Printf("Transfered table (%d/%d): %s", i, len(sourceTableNames), tableName)
	}

	logger.Println("Done!")
}

func uploadArkResultsCmd(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		logger.Printf("Upload command requires at exactly 2 arguments, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	destinationDSNString := viper.GetString("dsn")
	arkUser := viper.GetString("ark-user")
	arkPassword := viper.GetString("ark-password")
	destinationTableName := args[1]
	sourceDate, err := util.ParseDateTime(args[0])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err != nil {
		logger.Errorf("Given date is not in format 'YYYY-MM-DD': %s.\n", args[1])
		return
	}

	logger.Debugf("Running command for date %s source tables.\n", args[1])

	destinationClient, err := clientv3.NewNativeSQLClientWithPing(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database heathcheck failed: %v.\n", err)
		return
	}

	if tableSize, err := destinationClient.TableSize(ctx, destinationTableName); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty table exists in the destination, try --force flag.")
		return
	}

	sourceClient, err := clientv3.NewArkClient(arkUser, arkPassword)
	if err != nil {
		logger.Errorf("Ark client connection failed: %v.\n", err)
		return
	}

	destinationManager := pipeline.NewClickHouseManager[apiv3.IrisResultsRow](ctx, destinationClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       destinationTableName,
		AddCheckIfExist: true,
		Database:        destinationClient.Database,
	}, &queries.BasicCreateQuery{
		TableName:           destinationTableName,
		AddCheckIfNotExists: true,
		Database:            destinationClient.Database,
		Object:              apiv3.IrisResultsRow{},
	})
	if err != nil {
		logger.Errorf("Destination ClickHouse database table reset failed: %v.\n", err)
		return
	}

	sourceStreamer := pipeline.NewArkStreamer(ctx, sourceClient)
	ingestCh := sourceStreamer.Ingest(sourceDate, 1)

	destinationStreamer := pipeline.NewClickHouseRowStreamer[apiv3.IrisResultsRow](ctx, destinationClient)
	destinationStreamer.Egress(ingestCh, &queries.BasicInsertQuery{
		TableName: destinationTableName,
		Database:  destinationClient.Database,
		Object:    apiv3.IrisResultsRow{},
	}, 1)

	logger.Println("Started streaming.")

	var topGroup errgroup.Group
	topGroup.Go(sourceStreamer.G.Wait)
	// topGroup.Go(destinationStreamer.G.Wait)

	if err := topGroup.Wait(); err != nil {
		logger.Errorf("An error occured while transfering data %v.\n", err)
		return
	}
}
