package main

import (
	"context"
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

func computeCmd() *cobra.Command {
	computeCmd := &cobra.Command{
		Use:   "compute",
		Short: "Compute derived tables from source data",
	}

	computeCmd.AddCommand(computeResultsFiesCmd())

	return computeCmd
}

func computeResultsFiesCmd() *cobra.Command {
	var (
		policy        string
		chunkSize     int
		rttResolution float64
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
			)
		},
	}

	cmd.Flags().StringVar(&policy, "policy", "append", "Write policy: replace, truncate, fail, append")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", service.DefaultFIEChunkSize, "Number of destination prefixes per chunk")
	cmd.Flags().Float64Var(&rttResolution, "rtt-resolution", service.DefaultFIERTTResolution, "RTT resolution in milliseconds")

	return cmd
}

func runResultsFies(ctx context.Context, inputTable, outputTable, policy string, chunkSize int, rttResolution float64) error {
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
	})

	fmt.Printf("computing [results to fies]: %s.%s -> %s.%s\n", config.Database, inputTable, config.Database, outputTable)

	if err := svc.Compute(ctx, source, dest); err != nil {
		return fmt.Errorf("failed to compute fies: %w", err)
	}

	return nil
}
