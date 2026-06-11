package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/retina"
	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

func fetchRetinaFIEsCmd() *cobra.Command {
	var (
		policy    string
		timeout   time.Duration
		endpoint  string
		batchSize int
	)

	cmd := &cobra.Command{
		Use:   "retina-fies <dest-table>",
		Short: "Fetch Retina live stream FIEs into a destination table",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetchRetinaFIEs(
				cmd.Context(),
				args[0],
				policy,
				timeout,
				endpoint,
				batchSize,
			)
		},
	}

	cmd.Flags().StringVar(&policy, "policy", string(store.PreparationPolicyFail), "Write policy: replace, truncate, fail, append")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Stream timeout; 0 means no timeout")
	cmd.Flags().StringVar(&endpoint, "endpoint", retina.DefaultEndpoint, "Retina stream endpoint URL")
	cmd.Flags().IntVar(&batchSize, "batch-size", retina.DefaultBatchSize, "Number of FIEs to accumulate per insert batch")

	return cmd
}

func runFetchRetinaFIEs(
	ctx context.Context,
	destinationTable string,
	policy string,
	timeout time.Duration,
	endpoint string,
	batchSize int,
) error {
	// Apply timeout if set.
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Create store.
	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		return fmt.Errorf("failed to parse config from DSN: %w", err)
	}
	s, err := store.NewStore(config)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	// Create retina client.
	retinaClient := retina.NewRetinaClient(retina.Config{
		Endpoint:  endpoint,
		BatchSize: batchSize,
	})

	// Create and run service.
	svc := service.NewRetinaService(s, retinaClient, service.RetinaConfig{
		PreparationPolicy: store.PreparationPolicy(policy),
	})

	return svc.Stream(ctx, store.DatabaseTable{Database: config.Database, Table: destinationTable})
}
