package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

func main() {
	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		panic(err)
	}

	s, err := store.NewStore(config)
	if err != nil {
		panic(err)
	}

	svc := service.NewFIEComputeService(s, service.FIEComputeConfig{
		ChunkSize:         1_000_000,
		RTTResolution:     0.1,
		PreparationPolicy: store.PreparationPolicyFail,
	})

	source := store.DatabaseTable{
		Database: "mpat",
		Table:    "iris_results__20260601",
	}
	dest := store.DatabaseTable{
		Database: "mpat",
		Table:    "iris_fies__20260601_testttt",
	}

	if err := svc.Compute(context.Background(), source, dest); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "error: environment variable %s is required\n", key)
		os.Exit(1)
	}
	return v
}
