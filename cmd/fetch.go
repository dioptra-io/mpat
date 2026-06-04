package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/ripe"
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
	fetchCmd.AddCommand(fetchRipePrefixesCmd())
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
	defer func() { _ = irisClient.Logout() }()

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

func fetchRipePrefixesCmd() *cobra.Command {
	var (
		asnsFlag  string
		tier1     bool
		date      string
		snapshot  string
		timestamp string
		policy    string
		database  string
	)

	cmd := &cobra.Command{
		Use:   "ripe-prefixes <dest-table>",
		Short: "Fetch BGP prefixes from RIPE Stat into a destination table",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetchRipePrefixes(
				cmd.Context(),
				args[0],
				database,
				policy,
				asnsFlag,
				tier1,
				date,
				snapshot,
				timestamp,
			)
		},
	}

	cmd.Flags().StringVar(&asnsFlag, "asns", "", "Comma-separated list of ASNs (e.g. 3356,1299,3257)")
	cmd.Flags().BoolVar(&tier1, "tier1", false, "Use the hardcoded list of tier-1 ASNs")
	cmd.Flags().StringVar(&date, "date", "", "Date for the snapshot (e.g. 2026-06-01), used with --snapshot")
	cmd.Flags().StringVar(&snapshot, "snapshot", "dawn", "Time of day for the snapshot: dawn, day, night")
	cmd.Flags().StringVar(&timestamp, "timestamp", "", "Raw RFC3339 timestamp (e.g. 2026-06-01T08:00:00Z), alternative to --date + --snapshot")
	cmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
	cmd.Flags().StringVar(&database, "database", envOr("MPAT_DATABASE", store.DefaultDatabase), "ClickHouse database name")

	return cmd
}

func runFetchRipePrefixes(ctx context.Context, destTable, database, policy, asnsFlag string, tier1 bool, dateStr, snapshotStr, timestampStr string) error {
	// Validate ASN flags — exactly one of --asns or --tier1 must be set.
	if asnsFlag == "" && !tier1 {
		return fmt.Errorf("exactly one of --asns or --tier1 must be set")
	}
	if asnsFlag != "" && tier1 {
		return fmt.Errorf("--asns and --tier1 cannot be set at the same time")
	}

	// Validate time flags — exactly one of --timestamp or --date must be set.
	if timestampStr == "" && dateStr == "" {
		return fmt.Errorf("exactly one of --timestamp or --date must be set")
	}
	if timestampStr != "" && dateStr != "" {
		return fmt.Errorf("--timestamp and --date cannot be set at the same time")
	}

	// Resolve ASNs.
	var asns []uint32
	if tier1 {
		asns = ripe.Tier1ASNs
	} else {
		for _, s := range strings.Split(asnsFlag, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			n, err := strconv.ParseUint(s, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid ASN %q: %w", s, err)
			}
			asns = append(asns, uint32(n))
		}
		if len(asns) == 0 {
			return fmt.Errorf("--asns must contain at least one ASN")
		}
	}

	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		return fmt.Errorf("failed to parse config from DSN: %w", err)
	}
	s, err := store.NewStore(config)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	ripeClient := ripe.NewRipeClient(ripe.RipeConfig{
		Endpoint: envOr("MPAT_RIPE_STAT_ENDPOINT", ripe.DefaultEndpoint),
	})

	dest := store.DatabaseTable{
		Database: database,
		Table:    destTable,
	}

	svc := service.NewRipePrefixesService(s, ripeClient, service.RipePrefixesConfig{
		ASNs:              asns,
		PreparationPolicy: store.PreparationPolicy(policy),
	})

	if timestampStr != "" {
		t, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return fmt.Errorf("invalid --timestamp %q: %w", timestampStr, err)
		}
		return svc.FetchAt(ctx, dest, t)
	}

	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("invalid --date %q, expected format 2006-01-02: %w", dateStr, err)
	}

	return svc.Fetch(ctx, dest, date, ripe.TimeOfDay(snapshotStr))
}
