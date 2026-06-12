package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

func computeResultsFiesCmd() *cobra.Command {
	var (
		policy        string
		chunkSize     int
		rttResolution float64
		cardinality   string
		nullity       string
	)
	cmd := &cobra.Command{
		Use:   "fies <input-table> <output-table>",
		Short: "Compute forwarding info elements from iris results",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runResultsFies(
				cmd.Context(),
				args[0],
				args[1],
				policy,
				chunkSize,
				rttResolution,
				cardinality,
				nullity,
			)
		},
	}
	cmd.Flags().StringVar(&policy, "policy", "append", "Write policy: replace, truncate, fail, append")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", service.DefaultFIEChunkSize, "Number of destination prefixes per chunk")
	cmd.Flags().Float64Var(&rttResolution, "rtt-resolution", service.DefaultFIERTTResolution, "RTT resolution in milliseconds")
	cmd.Flags().StringVar(&cardinality, "cardinality", string(service.CardinalityOneToOne), "Cardinality policy: one_to_one, many_to_one, one_to_many, all")
	cmd.Flags().StringVar(&nullity, "nullity", string(service.NullityBothSome), "Nullity policy: both_some, far_none, any")
	return cmd
}

func runResultsFies(ctx context.Context, inputTable, outputTable, policy string, chunkSize int, rttResolution float64, cardinality, nullity string) error {
	log := slog.Default()

	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		return fmt.Errorf("failed to parse config from DSN: %w", err)
	}

	s, err := store.NewStore(config)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	source := store.DatabaseTable{
		Database: config.Database,
		Table:    inputTable,
	}
	dest := store.DatabaseTable{
		Database: config.Database,
		Table:    outputTable,
	}

	svc := service.NewFIEComputeService(s, service.FIEComputeConfig{
		ChunkSize:         chunkSize,
		RTTResolution:     rttResolution,
		PreparationPolicy: store.PreparationPolicy(policy),
		Cardinality:       service.CardinalityPolicy(cardinality),
		Nullity:           service.NullityPolicy(nullity),
	})

	log.InfoContext(ctx, "starting fie computation",
		"source", fmt.Sprintf("%s.%s", config.Database, inputTable),
		"dest", fmt.Sprintf("%s.%s", config.Database, outputTable),
		"policy", policy,
		"chunk_size", chunkSize,
		"rtt_resolution", rttResolution,
		"cardinality", cardinality,
		"nullity", nullity,
	)

	if err := svc.Compute(ctx, source, dest); err != nil {
		return fmt.Errorf("failed to compute fies: %w", err)
	}

	return nil
}
