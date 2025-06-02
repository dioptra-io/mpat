package upload

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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
		Use:   "iris-results <destination-table-name> [<source-table-names>...]",
		Short: "Copy the iris results table.",
		Long:  "Copy the iris results table from prod to working ClickHouse database.",
		Args:  cobra.ArbitraryArgs,
		Run:   uploadIrisResultsCmd,
	}
	uploadIrisResultsCmd.Flags().StringP("source-dsn", "s", "", "dsn string of production clickhouse database")
	viper.BindPFlag("source-dsn", uploadIrisResultsCmd.Flags().Lookup("source-dsn"))
	viper.BindEnv("source-dsn", "MPAT_PROD_DSN")

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
	destinationTableName := args[0]
	sourceTableNames := args[1:]

	logger.Debugf("Running command with %d source tables.\n", len(sourceTableNames))

	destinationClient, err := clientv2.NewSQLClientWithHealthCheck(destinationDSNString)
	if err != nil {
		logger.Errorf("Destination ClickHouse database heathcheck failed: %v.\n", err)
		return
	}

	sourceClient, err := clientv2.NewSQLClientWithHealthCheck(sourceDSNString)
	if err != nil {
		logger.Errorf("Source ClickHouse database heathcheck failed: %v.\n", err)
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

	sourceStreamer := pipeline.NewClickHouseStreamer[v3.IrisResultsRow](sourceClient)
	ingestCh, ingestErrCh := sourceStreamer.Ingest(&queries.BasicSelectQuery{
		TableNames: sourceTableNames,
	})

	destinationStreamer := pipeline.NewClickHouseStreamer[v3.IrisResultsRow](destinationClient)
	egressErrCh := destinationStreamer.Egress(ingestCh, ingestErrCh, &queries.BasicInsertQuery{
		TableName: destinationTableName,
	})

	for err := range egressErrCh {
		logger.Errorf("An error occured while transfering data %v.\n", err)
		return
	}

	logger.Println("Done!")
}
