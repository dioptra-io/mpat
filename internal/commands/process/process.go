package process

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	v3 "github.com/dioptra-io/ufuk-research/api/v3"
	"github.com/dioptra-io/ufuk-research/internal/pipeline"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func ProcessCmd() *cobra.Command {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Process data from ClickHouse to ClickHouse.",
		Long:  "Stream the data from the given table, process it, and upload it into ClickHouse database.",
		Args:  cobra.ArbitraryArgs,
		Run:   processCmd,
	}

	processForwardingDecision := &cobra.Command{
		Use:   "forwarding-decision <input-table> <output-table>",
		Short: "Compute forwarding decision",
		Long:  "Compute the forwarding decision table given in forwarding info design doc.",
		Args:  cobra.ArbitraryArgs,
		Run:   processForwardingDecisionCmd,
	}
	processForwardingDecision.Flags().IntP("parallel-workers", "w", config.DefaultNumParallelWorkersInPipeline, "number of parallel workers spawned")
	viper.BindPFlag("parallel-workers", processForwardingDecision.Flags().Lookup("parallel-workers"))

	processCmd.AddCommand(processForwardingDecision)

	return processCmd
}

func processCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func processForwardingDecisionCmd(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		logger.Printf("Process command requires at least 2 arguments, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	parallelWorkers := viper.GetInt("parallel-workers")
	clickHouseDSNString := viper.GetString("dsn")
	sourceTableName := args[0]
	destinationTableName := args[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clickHouseClient, err := clientv3.NewNativeSQLClient(clickHouseDSNString)
	if err != nil {
		logger.Errorf("ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	if tableSize, err := clickHouseClient.TableSize(ctx, destinationTableName); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty table exists in the destination, try --force flag.")
		return
	}

	destinationManager := pipeline.NewClickHouseManager[v3.IrisResultsRow](ctx, clickHouseClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       destinationTableName,
		AddCheckIfExist: true,
	}, &queries.BasicCreateQuery{
		TableName:           destinationTableName,
		AddCheckIfNotExists: true,
	})
	if err != nil {
		logger.Errorf("ClickHouse database table reset failed: %v.\n", err)
		return
	}

	sourceStreamer := pipeline.NewClickHouseRowStreamer[v3.IrisGroupedResultsRow](ctx, clickHouseClient)
	ingestCh := sourceStreamer.Ingest(&queries.BasicSelectQuery{
		TableNames: []string{sourceTableName},
	})

	processStreamer := pipeline.NewForwardingDecisionProcessor(ctx, parallelWorkers, 1000)
	processCh := processStreamer.Start(ingestCh)

	destinationStreamer := pipeline.NewClickHouseRowStreamer[v3.ForwardingDecisionRow](ctx, clickHouseClient)
	destinationStreamer.Egress(processCh, &queries.BasicInsertQuery{
		TableName: destinationTableName,
	}, parallelWorkers)

	var topGroup errgroup.Group
	topGroup.Go(sourceStreamer.G.Wait)
	topGroup.Go(processStreamer.G.Wait)
	topGroup.Go(destinationStreamer.G.Wait)

	if err := topGroup.Wait(); err != nil {
		logger.Errorf("An error occured while transfering data %v.\n", err)
		return
	}

	logger.Println("Done!")
}
