package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/ripe"
	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

func fetchRipePrefixesCmd() *cobra.Command {
	var (
		asnsFlag   string
		tier1      bool
		date       string
		snapshot   string
		timestamp  string
		policy     string
		database   string
		maxRetries int
		retryDelay time.Duration
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
				maxRetries,
				retryDelay,
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
	cmd.Flags().IntVar(&maxRetries, "max-retries", ripe.DefaultMaxRetries, "Maximum number of retry attempts on failure.")
	cmd.Flags().DurationVar(&retryDelay, "retry-delay", ripe.DefaultRetryDelay, "Duration to wait between retry attempts.")

	return cmd
}

func runFetchRipePrefixes(ctx context.Context, destTable, database, policy, asnsFlag string, tier1 bool, dateStr, snapshotStr, timestampStr string, maxRetries int, retryDelay time.Duration) error {
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
		for s := range strings.SplitSeq(asnsFlag, ",") {
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
		Endpoint:   envOr("MPAT_RIPE_STAT_ENDPOINT", ripe.DefaultEndpoint),
		MaxRetries: maxRetries,
		RetryDelay: retryDelay,
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
