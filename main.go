package main

import (
	"context"
	"fmt"
	"os"

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

	schema, err := s.TableSchema(context.Background(), store.DatabaseTable{
		Database: "mpat",
		Table:    "my_fies2",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(schema.Columns())
	fmt.Printf("schema.SchemaName(): %v\n", schema.SchemaName())
	fmt.Printf("schema.DDL(\"mpat\", \"new_table\"): %v\n", schema.DDL("mpat", "new_table"))
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "error: environment variable %s is required\n", key)
		os.Exit(1)
	}
	return v
}
