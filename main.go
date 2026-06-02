package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

func main() {
	// ── Iris client ───────────────────────────────────────────────────────────
	irisClient, err := iris.NewIrisClient(iris.Config{
		Username: mustEnv("IRIS_USERNAME"),
		Password: mustEnv("IRIS_PASSWORD"),
		Endpoint: envOr("IRIS_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("failed to create iris client: %v", err)
	}
	defer irisClient.Logout()
	fmt.Println("==> Iris client ready")

	// ── Store ─────────────────────────────────────────────────────────────────
	s, err := store.NewStore(store.StoreConfig{
		ConnectionString: mustEnv("MPAT_CLICKHOUSE"),
	})
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	fmt.Println("==> Store ready")

	// ── Pick the first finished measurement with at least one agent ───────────
	measurements, err := irisClient.Measurements().
		State(iris.StateFinished).
		Between(time.Now().AddDate(0, 0, -1), time.Now()).
		Fetch()
	if err != nil {
		log.Fatalf("failed to fetch measurements: %v", err)
	}
	fmt.Printf("==> Found %d finished measurements\n", len(measurements))

	var sourceTable string
	for _, m := range measurements {
		groups := iris.TableGroupsForMeasurement(m)
		if len(groups) > 0 {
			sourceTable = groups[0].Results.TableName
			fmt.Printf("==> Using source table: %s\n", sourceTable)
			break
		}
	}
	if sourceTable == "" {
		log.Fatalf("no source table found")
	}

	// ── Destination ───────────────────────────────────────────────────────────
	dest := store.DestTable{
		Database: "mpat",
		Table:    "test_results",
	}

	schema, err := store.ResultsDDL(dest)
	if err != nil {
		log.Fatalf("failed to render schema: %v", err)
	}

	// ── Fetch rows from Iris ──────────────────────────────────────────────────
	fmt.Printf("==> Querying source table...\n")
	rows, err := irisClient.Query().
		Select(fmt.Sprintf("SELECT * FROM %s LIMIT 100", sourceTable)).
		Json()
	if err != nil {
		log.Fatalf("failed to query source table: %v", err)
	}

	// ── Write to local ClickHouse ─────────────────────────────────────────────
	fmt.Printf("==> Writing to %s.%s (policy: replace)...\n", dest.Database, dest.Table)
	if err := s.Put(store.PolicyReplace, dest, schema, rows); err != nil {
		log.Fatalf("failed to put rows: %v", err)
	}

	fmt.Println("==> Done!")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("environment variable %s is required", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
