package main

// import (
// 	"context"
// 	"fmt"
//
// 	"github.com/dioptra-io/ufuk-research/internal/store"
// 	"github.com/spf13/cobra"
// )
//
// func computeCmd() *cobra.Command {
// 	computeCmd := &cobra.Command{
// 		Use:   "compute",
// 		Short: "Compute derived tables from source data",
// 	}
//
// 	computeCmd.AddCommand(computeResultsFiesCmd())
//
// 	return computeCmd
// }
//
// func computeResultsFiesCmd() *cobra.Command {
// 	var (
// 		policy        string
// 		database      string
// 		chunkSize     int
// 		rttResolution float64
// 	)
//
// 	cmd := &cobra.Command{
// 		Use:   "results-fies <input-table> <output-table>",
// 		Short: "Compute forwarding info elements from iris results",
// 		Args:  cobra.ExactArgs(2),
// 		RunE: func(cmd *cobra.Command, args []string) error {
// 			return runResultsFies(
// 				cmd.Context(),
// 				args[0],
// 				args[1],
// 				database,
// 				chunkSize,
// 				rttResolution,
// 				policy,
// 			)
// 		},
// 	}
//
// 	cmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
// 	cmd.Flags().StringVar(&database, "database", envOr("MPAT_DATABASE", store.DefaultDatabase), "ClickHouse database name")
// 	cmd.Flags().IntVar(&chunkSize, "chunk-size", store.DefaultFIEChunkSize, "Number of destination prefixes per chunk")
// 	cmd.Flags().Float64Var(&rttResolution, "rtt-resolution", store.DefaultIrisRTTResolution, "RTT resolution in milliseconds")
//
// 	return cmd
// }
//
// func runResultsFies(ctx context.Context, inputTable, outputTable, database string, chunkSize int, rttResolution float64, policy string) error {
// 	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
// 	if err != nil {
// 		return fmt.Errorf("failed to parse config from DSN: %w", err)
// 	}
//
// 	s, err := store.NewStore(config)
// 	if err != nil {
// 		return fmt.Errorf("failed to create store: %w", err)
// 	}
//
// 	dest := store.DatabaseTable{
// 		Database: database,
// 		Table:    outputTable,
// 	}
//
// 	cfg := store.FiesConfig{
// 		SourceTable:   inputTable,
// 		ChunkSize:     chunkSize,
// 		RTTResolution: rttResolution,
// 	}
//
// 	fmt.Printf("computing [results to fies]: %s -> %s.%s\n", inputTable, database, outputTable)
// 	fiesSchema, err := store.FIESSchema(dest)
// 	if err != nil {
// 		return fmt.Errorf("failed to create fies schema: %w", err)
// 	}
// 	if err := s.PrepareTable(store.PreparationPolicy(policy), dest, fiesSchema); err != nil {
// 		return fmt.Errorf("failed to create fies table: %w", err)
// 	}
//
// 	if err := s.GenerateFies(ctx, dest, cfg); err != nil {
// 		return fmt.Errorf("failed to generate fies: %w", err)
// 	}
//
// 	return nil
// }
