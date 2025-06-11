package upload

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	v1 "github.com/dioptra-io/ufuk-research/api/v1"
	v3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/internal/pipeline"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
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
		Use:   "iris-results <destination-table-name> <source-date>",
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
		Use:   "ark-results <destination-table-name> <source-date>",
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
	destinationDSNString := viper.GetString("dsn")
	sourceDSNString := viper.GetString("source-dsn")
	onlyIPv4Measurements := viper.GetBool("ipv4")
	destinationTableName := args[0]

	sourceDate, err := v1.ParseArkDate(args[1])
	if err != nil {
		logger.Errorf("Cannot parse given date: %v\n.", err)
		return
	}

	irisClient, err := clientv2.NewIrisClientWithJWT()
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

	destinationClient, err := clientv2.NewSQLClientWithHealthCheck(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	sourceClient, err := clientv2.NewSQLClientWithHealthCheck(sourceDSNString)
	if err != nil {
		logger.Errorf("Source ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	destinationManager := pipeline.NewClickHouseManager[v3.IrisResultsRow](destinationClient)
	err = destinationManager.DeleteThenCreate(true, &queries.BasicDeleteQuery{
		TableName:       destinationTableName,
		AddCheckIfExist: true,
	}, &queries.BasicCreateQuery{
		TableName:           destinationTableName,
		AddCheckIfNotExists: true,
	})
	if err != nil {
		panic(err)
	}

	sourceStreamer := pipeline.NewClickHouseStreamer[v3.IrisResultsRow](sourceClient)
	ingestCh, ingestErrCh := sourceStreamer.Ingest(&queries.BasicSelectQuery{
		TableNames: sourceTableNames,
	})

	destinationStreamer := pipeline.NewClickHouseStreamer[v3.IrisResultsRow](destinationClient)
	egressErrCh := destinationStreamer.Egress(ingestCh, ingestErrCh, &queries.BasicInsertQuery{
		TableName: destinationTableName,
	})

	logger.Println("Started streaming data.")

	for err := range egressErrCh {
		logger.Errorf("An error occured while transfering data %v.\n", err)
		return
	}

	logger.Println("Done!")
}

func uploadArkResultsCmd(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		logger.Printf("Upload command requires at exactly 2 arguments, got %d", len(args))
		return
	}
	destinationDSNString := viper.GetString("dsn")
	arkUser := viper.GetString("ark-user")
	arkPassword := viper.GetString("ark-password")
	destinationTableName := args[0]
	sourceDate, err := util.ParseDateTime(args[1])
	if err != nil {
		logger.Errorf("Given date is not in format 'YYYY-MM-DD': %s.\n", args[1])
		return
	}

	logger.Debugf("Running command for date %s source tables.\n", args[1])

	destinationClient, err := clientv2.NewSQLClientWithHealthCheck(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database heathcheck failed: %v.\n", err)
		return
	}

	sourceClient, err := clientv2.NewArkClient(arkUser, arkPassword)
	if err != nil {
		logger.Errorf("Ark client connection failed: %v.\n", err)
		return
	}

	destinationManager := pipeline.NewClickHouseManager[v3.IrisResultsRow](destinationClient)
	err = destinationManager.DeleteThenCreate(true, &queries.BasicDeleteQuery{
		TableName:       destinationTableName,
		AddCheckIfExist: true,
	}, &queries.BasicCreateQuery{
		TableName:           destinationTableName,
		AddCheckIfNotExists: true,
	})
	if err != nil {
		panic(err)
	}

	sourceStreamer := pipeline.NewArkStreamer(sourceClient)
	ingestCh, ingestErrCh := sourceStreamer.Ingest(sourceDate)

	destinationStreamer := pipeline.NewClickHouseStreamer[v3.IrisResultsRow](destinationClient)
	destinationStreamer.EgressChunkSize = 10
	egressErrCh := destinationStreamer.Egress(ingestCh, ingestErrCh, &queries.BasicInsertQuery{
		TableName: destinationTableName,
	})

	for err := range egressErrCh {
		logger.Errorf("An error occured while transfering data %v.\n", err)
		return
	}

	logger.Println("Done!")
}
