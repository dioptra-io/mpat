package process

import (
	"context"
	"fmt"
	"math"
	"strconv"

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
		Use:   "forwarding-decision <stem>",
		Short: "Compute forwarding decision",
		Long:  "Compute the forwarding decision table given in forwarding info design doc.",
		Args:  cobra.ArbitraryArgs,
		Run:   processForwardingDecisionCmd,
	}
	processForwardingDecision.Flags().IntP("parallel-workers", "w", config.DefaultNumParallelWorkersInPipeline, "number of parallel workers spawned")
	viper.BindPFlag("parallel-workers", processForwardingDecision.Flags().Lookup("parallel-workers"))

	processPrefixes := &cobra.Command{
		Use:   "prefixes <stem>",
		Short: "Compute score",
		Long:  "Compute the scores table given in forwarding info design doc.",
		Args:  cobra.ArbitraryArgs,
		Run:   processPrefixesCmd,
	}

	processForwardingInfo := &cobra.Command{
		Use:   "scores <stem>",
		Short: "Compute score",
		Long:  "Compute the scores table given in forwarding info design doc.",
		Args:  cobra.ArbitraryArgs,
		Run:   processScoresCmd,
	}

	processCmd.AddCommand(processForwardingDecision)
	processCmd.AddCommand(processForwardingInfo)
	processCmd.AddCommand(processPrefixes)

	return processCmd
}

func processCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func processForwardingDecisionCmd(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		logger.Printf("Process command requires 1 argument, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	parallelWorkers := viper.GetInt("parallel-workers")
	numPrefixesPerChunk := 100000
	clickHouseDSNString := viper.GetString("dsn")
	stemName := args[0]
	routeTraceTable := fmt.Sprintf("%s__rt", stemName)
	forwardingDecisionTable := fmt.Sprintf("%s__fd", stemName)
	prefixTable := fmt.Sprintf("%s__pf", stemName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clickHouseClient, err := clientv3.NewNativeSQLClient(clickHouseDSNString)
	if err != nil {
		logger.Errorf("ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	if tableSize, err := clickHouseClient.TableSize(ctx, forwardingDecisionTable); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty source table exists in the destination, try --force flag.")
		return
	}

	cmd.Flags().Lookup("force").Value.Set("true")
	processPrefixesCmd(cmd, []string{stemName})
	cmd.Flags().Lookup("force").Value.Set(strconv.FormatBool(force)) // restore

	numDestinationPrefixes, err := clickHouseClient.NumRows(ctx, prefixTable)
	if err != nil {
		logger.Errorf("Destination ClickHouse prefix table check failed: %v.\n", err)
		return
	}
	totalIterations := int(math.Ceil(float64(numDestinationPrefixes) / float64(numPrefixesPerChunk)))
	logger.Printf("Going to chunk by %d prefixes, total of %d", totalIterations, numDestinationPrefixes)

	destinationManager := pipeline.NewClickHouseManager[v3.ForwardingDecisionRow](ctx, clickHouseClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       forwardingDecisionTable,
		AddCheckIfExist: true,
		Database:        clickHouseClient.Database,
	}, &queries.BasicCreateQuery{
		TableName:           forwardingDecisionTable,
		AddCheckIfNotExists: true,
		Database:            clickHouseClient.Database,
		Object:              v3.ForwardingDecisionRow{},
	})
	if err != nil {
		logger.Errorf("ClickHouse database table reset failed: %v.\n", err)
		return
	}

	logger.Println("Started processing in chunks.")

	i := 0

	for offset := 0; offset < totalIterations; offset++ {
		i++
		sourceStreamer := pipeline.NewClickHouseRowStreamer[v3.GrouppedForwardingDecisionResultsRow](ctx, clickHouseClient)
		ingestCh := sourceStreamer.Ingest(&queries.GrouppedForwardingDecisionSelectQuery{
			TableName:   routeTraceTable,
			PrefixTable: prefixTable,
			Database:    clickHouseClient.Database,
			Limit:       numPrefixesPerChunk,
			Offset:      offset,
		})

		processStreamer := pipeline.NewForwardingDecisionProcessor(ctx, parallelWorkers, 1000000)
		processCh := processStreamer.Start(ingestCh)

		destinationStreamer := pipeline.NewClickHouseRowStreamer[v3.ForwardingDecisionRow](ctx, clickHouseClient)
		destinationStreamer.Egress(processCh, &queries.BasicInsertQuery{
			TableName: forwardingDecisionTable,
			Database:  clickHouseClient.Database,
			Object:    v3.ForwardingDecisionRow{},
		}, 1)

		var topGroup errgroup.Group
		topGroup.Go(sourceStreamer.G.Wait)
		topGroup.Go(processStreamer.G.Wait)
		topGroup.Go(destinationStreamer.G.Wait)

		if err := topGroup.Wait(); err != nil {
			logger.Errorf("An error occured while transfering data: %v.\n", err)
			return
		}

		logger.Printf("Chunk %d completed.\n", i)
	}

	logger.Println("Done!")
}

func processPrefixesCmd(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		logger.Printf("Process command requires 1 argument, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	clickHouseDSNString := viper.GetString("dsn")
	stemName := args[0]
	routeTraceTable := fmt.Sprintf("%s__rt", stemName)
	prefixTable := fmt.Sprintf("%s__pf", stemName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clickHouseClient, err := clientv3.NewNativeSQLClient(clickHouseDSNString)
	if err != nil {
		logger.Errorf("ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	if tableSize, err := clickHouseClient.TableSize(ctx, prefixTable); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty table exists in the destination, try --force flag.")
		return
	}

	destinationManager := pipeline.NewClickHouseManager[v3.PFRow](ctx, clickHouseClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       prefixTable,
		AddCheckIfExist: true,
		Database:        clickHouseClient.Database,
	}, &queries.BasicCreateQuery{
		TableName:           prefixTable,
		AddCheckIfNotExists: true,
		Database:            clickHouseClient.Database,
		Object:              v3.PFRow{},
	})
	if err != nil {
		logger.Errorf("ClickHouse database table reset failed: %v.\n", err)
		return
	}

	logger.Println("Started processing.")

	if err := destinationManager.Execute(&queries.InsertFromUniquePrefixes{
		TableNameToInsert: prefixTable,
		TableNameToSelect: routeTraceTable,
		Database:          clickHouseClient.Database,
	}); err != nil {
		logger.Errorf("ClickHouse insert failed: %v.\n", err)
		return
	}

	logger.Println("Done!")
}

func processScoresCmd(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		logger.Printf("Process command requires 1 argument, got %d", len(args))
		return
	}
	force := viper.GetBool("force")
	clickHouseDSNString := viper.GetString("dsn")
	stemName := args[0]
	forwardingDecisionTable := fmt.Sprintf("%s__fd", stemName)
	scoreTable := fmt.Sprintf("%s__sc", stemName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clickHouseClient, err := clientv3.NewNativeSQLClient(clickHouseDSNString)
	if err != nil {
		logger.Errorf("ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	if tableSize, err := clickHouseClient.TableSize(ctx, scoreTable); err != nil {
		logger.Errorf("Destination ClickHouse database table check failed: %v.\n", err)
		return
	} else if tableSize > 0 && !force {
		logger.Errorf("Non-empty table exists in the destination, try --force flag.")
		return
	}

	destinationManager := pipeline.NewClickHouseManager[v3.ScoresRow](ctx, clickHouseClient)
	err = destinationManager.DeleteThenCreate(&queries.BasicDeleteQuery{
		TableName:       scoreTable,
		AddCheckIfExist: true,
		Database:        clickHouseClient.Database,
	}, &queries.BasicCreateQuery{
		TableName:           scoreTable,
		AddCheckIfNotExists: true,
		Database:            clickHouseClient.Database,
		Object:              v3.ScoresRow{},
	})
	if err != nil {
		logger.Errorf("ClickHouse database table reset failed: %v.\n", err)
		return
	}

	logger.Println("Started processing.")

	if err := destinationManager.Execute(&queries.InsertFromScores{
		TableNameToInsert: scoreTable,
		TableNameToSelect: forwardingDecisionTable,
		Database:          clickHouseClient.Database,
	}); err != nil {
		logger.Errorf("ClickHouse insert failed: %v.\n", err)
		return
	}

	logger.Println("Done!")
}
