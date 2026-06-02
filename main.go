package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
)

func main() {
	client, err := iris.NewIrisClient(iris.Config{
		Username: mustEnv("IRIS_USERNAME"),
		Password: mustEnv("IRIS_PASSWORD"),
		Endpoint: envOr("IRIS_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Logout()

	measurements, err := client.Measurements().
		Between(time.Now().AddDate(0, 0, -2), time.Now()).
		Fetch()
	if err != nil {
		log.Fatalf("failed to fetch measurements: %v", err)
	}

	fmt.Printf("found %d measurements in the last 2 days\n\n", len(measurements))
	for _, m := range measurements {
		fmt.Printf("%s  %-12s  %-14s  %s\n",
			m.CreationTime.Time.Format(time.RFC3339),
			m.State,
			m.Tool,
			m.UUID,
		)
	}
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
