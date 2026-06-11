package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

type MeasurementKind string

const (
	KindZeph MeasurementKind = "zeph"
	KindIPv6 MeasurementKind = "ipv6"
)

func (k MeasurementKind) isValid() bool {
	switch k {
	case KindZeph, KindIPv6:
		return true
	}
	return false
}

func (k MeasurementKind) tag() string {
	return string(k)
}

func (k MeasurementKind) ipVersion() uint8 {
	if k == KindIPv6 {
		return 6
	}
	return 4
}

func fetchIrisResultsCmd() *cobra.Command {
	var (
		policy       string
		tableFlag    string
		measurement  string
		from         string
		to           string
		date         string
		kind         string
		index        int
		state        string
		tag          string
		chunkSize    int
		ewmaAlpha    float64
		lite         bool
		database     string
		filterSource bool
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
				date,
				kind,
				index,
				state,
				tag,
				chunkSize,
				ewmaAlpha,
				lite,
				filterSource,
			)
		},
	}

	cmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
	cmd.Flags().StringVar(&database, "database", envOr("MPAT_DATABASE", store.DefaultDatabase), "ClickHouse database name")
	cmd.Flags().StringVar(&tableFlag, "table", "", "Source table name (mode 1)")
	cmd.Flags().StringVar(&measurement, "measurement", "", "Measurement UUID (mode 2)")
	cmd.Flags().StringVar(&from, "from", "", "Start date, RFC3339 (mode 3)")
	cmd.Flags().StringVar(&to, "to", "", "End date, RFC3339 (mode 3)")
	cmd.Flags().StringVar(&date, "date", "", "Date, YYYY-MM-DD (mode 4)")
	cmd.Flags().StringVar(&kind, "kind", "", "Measurement kind: zeph, ipv6 (mode 4, required)")
	cmd.Flags().IntVar(&index, "index", -1, "Index of the measurement to fetch, ordered by creation time (mode 4, required, 0-based)")
	cmd.Flags().StringVar(&state, "state", "finished", "Measurement state filter (mode 3 and 4)")
	cmd.Flags().StringVar(&tag, "tag", "", "Tag regex filter (mode 3)")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", service.DefaultFetchChunkSize, "Streaming chunk size")
	cmd.Flags().Float64Var(&ewmaAlpha, "ewma-alpha", 0.2, "Alpha parameter for ETA estimation")
	cmd.Flags().BoolVar(&lite, "lite", true, "Use ResultsLiteSchema instead of ResultsSchema")
	cmd.Flags().BoolVar(&filterSource, "filter-source", true, "Exclude rows whose IP version does not match the kind (zeph → IPv4, ipv6 → IPv6).")

	return cmd
}

func runFetchIrisResults(ctx context.Context, destTable, database, policy, tableFlag, measurement, fromStr, toStr, dateStr, kindStr string, index int, stateStr, tagPattern string, chunkSize int, ewmaAlpha float64, lite bool, filterSource bool) error {
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
	if dateStr != "" {
		modes++
	}
	if modes != 1 {
		return fmt.Errorf("exactly one of --table, --measurement, --from/--to, or --date must be set")
	}
	if (fromStr == "") != (toStr == "") {
		return fmt.Errorf("--from and --to must be set together")
	}

	// Mode 4 requires --kind and --index.
	if dateStr != "" {
		if kindStr == "" {
			return fmt.Errorf("--kind is required when --date is set")
		}
		if index < 0 {
			return fmt.Errorf("--index is required when --date is set")
		}
		k := MeasurementKind(kindStr)
		if !k.isValid() {
			return fmt.Errorf("invalid --kind value %q: must be one of zeph, ipv6", kindStr)
		}
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

	case dateStr != "":
		k := MeasurementKind(kindStr)
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return fmt.Errorf("invalid --date value %q: must be YYYY-MM-DD", dateStr)
		}
		start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		end := time.Date(date.Year(), date.Month(), date.Day(), 23, 59, 59, 0, time.UTC)
		q := irisClient.Measurements().Between(start, end)
		if stateStr != "" {
			q = q.State(iris.MeasurementAgentState(stateStr))
		}
		q = q.TagContains(k.tag())
		measurements, err := q.Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		sort.Slice(measurements, func(i, j int) bool {
			return measurements[i].CreationTime.Before(measurements[j].CreationTime.Time)
		})
		if index >= len(measurements) {
			return fmt.Errorf("--index %d is out of range: only %d measurement(s) found for date %s and kind %s", index, len(measurements), dateStr, kindStr)
		}
		for _, g := range iris.TableGroupsForMeasurement(measurements[index]) {
			sourceNames = append(sourceNames, g.Results.TableName)
		}
		if len(sourceNames) == 0 {
			return fmt.Errorf("no results tables found for date %s, kind %s, index %d", dateStr, kindStr, index)
		}
	}

	dest := store.DatabaseTable{
		Database: database,
		Table:    destTable,
	}

	var ipVersion uint8
	if filterSource && dateStr != "" {
		ipVersion = MeasurementKind(kindStr).ipVersion()
	}

	svc := service.NewFetchService(s, irisClient, service.FetchConfig{
		ChunkSize:         chunkSize,
		PreparationPolicy: store.PreparationPolicy(policy),
		Lite:              lite,
		EWMAAlpha:         ewmaAlpha,
		IPVersion:         ipVersion,
	})

	return svc.Fetch(ctx, sourceNames, dest)
}
