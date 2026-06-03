package main

import (
	"context"
	"log"
	"os"

	"github.com/dioptra-io/ufuk-research/internal/store"
)

func main() {
	dsn := os.Getenv("MPAT_CLICKHOUSE")
	if dsn == "" {
		log.Fatal("MPAT_CLICKHOUSE environment variable is not set")
	}

	s, err := store.NewStore(store.StoreConfig{
		ConnectionString: dsn,
	})
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	dest := store.DestTable{
		Database: "mpat",
		Table:    "fies",
	}

	cfg := store.DefaultFiesConfig("iris_results__20260601")

	log.Println("creating fies table...")
	if err := s.CreateFiesTable(ctx, dest); err != nil {
		log.Fatalf("failed to create fies table: %v", err)
	}
	log.Println("fies table ready")

	log.Println("generating fies...")
	if err := s.GenerateFies(ctx, dest, cfg); err != nil {
		log.Fatalf("failed to generate fies: %v", err)
	}
}
