package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

func fetchCmd() *cobra.Command {
	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch data from a source",
	}
	fetchCmd.AddCommand(fetchIrisResultsCmd())
	return fetchCmd
}

func fetchIrisResultsCmd() *cobra.Command {
	var (
		policy      string
		tableFlag   string
		measurement string
		from        string
		to          string
		state       string
		tag         string
		chunkSize   int
		ewmaAlpha   float64
		lite        bool
		database    string
	)

	cmd := &cobra.Command{
		Use:   "iris-results <dest-table>",
		Short: "Fetch iris results into a destination table",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetchIrisResults(
				cmd.Context(),
				args[0],
				database,
				policy,
				tableFlag,
				measurement,
				from,
				to,
				state,
				tag,
				chunkSize,
				ewmaAlpha,
				lite,
			)
		},
	}

	cmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
	cmd.Flags().StringVar(&database, "database", envOr("MPAT_DATABASE", store.DefaultDatabase), "ClickHouse database name")
	cmd.Flags().StringVar(&tableFlag, "table", "", "Source table name (mode 1)")
	cmd.Flags().StringVar(&measurement, "measurement", "", "Measurement UUID (mode 2)")
	cmd.Flags().StringVar(&from, "from", "", "Start date, RFC3339 (mode 3)")
	cmd.Flags().StringVar(&to, "to", "", "End date, RFC3339 (mode 3)")
	cmd.Flags().StringVar(&state, "state", "finished", "Measurement state filter for mode 3")
	cmd.Flags().StringVar(&tag, "tag", "", "Tag regex filter for mode 3")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", service.DefaultFetchChunkSize, "Streaming chunk size")
	cmd.Flags().Float64Var(&ewmaAlpha, "ewma-alpha", 0.2, "Alpha parameter for ETA estimation")
	cmd.Flags().BoolVar(&lite, "lite", true, "Use ResultsLiteSchema instead of ResultsSchema")

	return cmd
}

func runFetchIrisResults(ctx context.Context, destTable, database, policy, tableFlag, measurement, fromStr, toStr, stateStr, tagPattern string, chunkSize int, ewmaAlpha float64, lite bool) error {
	modes := 0
	if tableFlag != "" {
		modes++
	}
	if measurement != "" {
		modes++
	}
	if fromStr != "" || toStr != "" {
		modes++
	}
	if modes != 1 {
		return fmt.Errorf("exactly one of --table, --measurement, or --from/--to must be set")
	}
	if (fromStr == "") != (toStr == "") {
		return fmt.Errorf("--from and --to must be set together")
	}

	irisClient, err := iris.NewIrisClient(iris.Config{
		Username: mustEnv("IRIS_USERNAME"),
		Password: mustEnv("IRIS_PASSWORD"),
		Endpoint: envOr("IRIS_ENDPOINT", iris.DefaultEndpoint),
	})
	if err != nil {
		return fmt.Errorf("failed to create iris client: %w", err)
	}
	defer irisClient.Logout()

	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		return fmt.Errorf("failed to parse config from DSN: %w", err)
	}

	s, err := store.NewStore(config)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	// Resolve source table names.
	var sourceNames []string
	switch {
	case tableFlag != "":
		sourceNames = []string{tableFlag}

	case measurement != "":
		measurements, err := irisClient.Measurements().Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			if m.UUID == measurement {
				for _, g := range iris.TableGroupsForMeasurement(m) {
					sourceNames = append(sourceNames, g.Results.TableName)
				}
				break
			}
		}
		if len(sourceNames) == 0 {
			return fmt.Errorf("no results tables found for measurement %s", measurement)
		}

	case fromStr != "":
		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return fmt.Errorf("invalid --from date %q: %w", fromStr, err)
		}
		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return fmt.Errorf("invalid --to date %q: %w", toStr, err)
		}
		q := irisClient.Measurements().Between(from, to)
		if stateStr != "" {
			q = q.State(iris.MeasurementAgentState(stateStr))
		}
		if tagPattern != "" {
			q = q.TagContains(tagPattern)
		}
		measurements, err := q.Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			for _, g := range iris.TableGroupsForMeasurement(m) {
				sourceNames = append(sourceNames, g.Results.TableName)
			}
		}
		if len(sourceNames) == 0 {
			return fmt.Errorf("no results tables found in range %s to %s", fromStr, toStr)
		}
	}

	dest := store.DatabaseTable{
		Database: database,
		Table:    destTable,
	}

	svc := service.NewFetchService(s, irisClient, service.FetchConfig{
		ChunkSize:         chunkSize,
		PreparationPolicy: store.PreparationPolicy(policy),
		Lite:              lite,
		EWMAAlpha:         ewmaAlpha,
	})

	return svc.Fetch(ctx, sourceNames, dest)
}
