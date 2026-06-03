package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/service"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

func main() {
	irisClient, err := iris.NewIrisClient(iris.Config{
		Username: mustEnv("IRIS_USERNAME"),
		Password: mustEnv("IRIS_PASSWORD"),
		Endpoint: envOr("IRIS_ENDPOINT", ""),
	})
	if err != nil {
		panic(err)
	}
	defer irisClient.Logout()

	config, err := store.ConfigFromDSN(mustEnv("MPAT_CLICKHOUSE"))
	if err != nil {
		panic(err)
	}

	s, err := store.NewStore(config)
	if err != nil {
		panic(err)
	}

	// Fetch measurements for a specific date range.
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)

	measurements, err := irisClient.Measurements().Between(from, to).Fetch()
	if err != nil {
		panic(err)
	}

	var sourceNames []string
	for _, m := range measurements {
		for _, g := range iris.TableGroupsForMeasurement(m) {
			sourceNames = append(sourceNames, g.Results.TableName)
		}
	}

	if len(sourceNames) == 0 {
		fmt.Fprintln(os.Stderr, "no source tables found")
		os.Exit(1)
	}

	fmt.Printf("found %d source table(s)\n", len(sourceNames))

	svc := service.NewFetchService(s, irisClient, service.FetchConfig{
		ChunkSize:         service.DefaultFetchChunkSize,
		PreparationPolicy: store.PreparationPolicyFail,
		Lite:              true,
		EWMAAlpha:         0.2,
	})

	dest := store.DatabaseTable{
		Database: envOr("MPAT_DATABASE", store.DefaultDatabase),
		Table:    "iris_resultslite__test",
	}

	if err := svc.Fetch(context.Background(), sourceNames, dest); err != nil {
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
