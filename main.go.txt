package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/spf13/cobra"
)

const (
	chunkSize = 500_000 // 500k rows per chunk
	ewmaAlpha = 0.2
)

func main() {
	var (
		policy      string
		tableFlag   string
		measurement string
		from        string
		to          string
		state       string
		tag         string
	)

	rootCmd := &cobra.Command{
		Use:          "mp",
		Short:        "Measurement Platform Analysis Tool",
		SilenceUsage: true,
	}

	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch data from a source",
	}

	irisResultsCmd := &cobra.Command{
		Use:   "iris-results <dest-table>",
		Short: "Fetch iris results into a destination table",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIrisResults(
				args[0],
				store.Policy(policy),
				tableFlag, measurement, from, to, state, tag,
			)
		},
	}

	irisResultsCmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
	irisResultsCmd.Flags().StringVar(&tableFlag, "table", "", "Source table name (mode 1)")
	irisResultsCmd.Flags().StringVar(&measurement, "measurement", "", "Measurement UUID (mode 2)")
	irisResultsCmd.Flags().StringVar(&from, "from", "", "Start date, RFC3339 (mode 3)")
	irisResultsCmd.Flags().StringVar(&to, "to", "", "End date, RFC3339 (mode 3)")
	irisResultsCmd.Flags().StringVar(&state, "state", "finished", "Measurement state filter for mode 3 (default: finished)")
	irisResultsCmd.Flags().StringVar(&tag, "tag", "", "Tag regex filter for mode 3 (optional)")

	fetchCmd.AddCommand(irisResultsCmd)
	rootCmd.AddCommand(fetchCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// tableInfo holds pre-scanned metadata for a source table.
type tableInfo struct {
	name   string
	total  int64
	chunks int64
}

func runIrisResults(destTable string, policy store.Policy, tableFlag, measurement, fromStr, toStr, stateStr, tagPattern string) error {
	// ── Validate flags ────────────────────────────────────────────────────────
	modes := 0
	if tableFlag != "" {
		modes++
	}
	if measurement != "" {
		modes++
	}
	if fromStr != "" || toStr != "" {
		modes++
	}
	if modes != 1 {
		return fmt.Errorf("exactly one of --table, --measurement, or --from/--to must be set")
	}
	if (fromStr == "") != (toStr == "") {
		return fmt.Errorf("--from and --to must be set together")
	}

	// ── Iris client ───────────────────────────────────────────────────────────
	irisClient, err := iris.NewIrisClient(iris.Config{
		Username: mustEnv("IRIS_USERNAME"),
		Password: mustEnv("IRIS_PASSWORD"),
		Endpoint: envOr("IRIS_ENDPOINT", ""),
	})
	if err != nil {
		return fmt.Errorf("failed to create iris client: %w", err)
	}
	defer irisClient.Logout()

	// ── Store ─────────────────────────────────────────────────────────────────
	s, err := store.NewStore(store.StoreConfig{
		ConnectionString: mustEnv("MPAT_CLICKHOUSE"),
	})
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	dest := store.DestTable{
		Database: envOr("MPAT_DATABASE", "mpat"),
		Table:    destTable,
	}

	schema, err := store.ResultsDDL(dest)
	if err != nil {
		return fmt.Errorf("failed to render schema: %w", err)
	}

	// ── Resolve source tables ─────────────────────────────────────────────────
	var sourceNames []string

	switch {
	case tableFlag != "":
		sourceNames = []string{tableFlag}

	case measurement != "":
		measurements, err := irisClient.Measurements().Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			if m.UUID == measurement {
				for _, g := range iris.TableGroupsForMeasurement(m) {
					sourceNames = append(sourceNames, g.Results.TableName)
				}
				break
			}
		}
		if len(sourceNames) == 0 {
			return fmt.Errorf("no results tables found for measurement %s", measurement)
		}

	case fromStr != "":
		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return fmt.Errorf("invalid --from date %q: %w", fromStr, err)
		}
		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return fmt.Errorf("invalid --to date %q: %w", toStr, err)
		}
		q := irisClient.Measurements().Between(from, to)
		if stateStr != "" {
			q = q.State(iris.MeasurementAgentState(stateStr))
		}
		if tagPattern != "" {
			q = q.TagContains(tagPattern)
		}
		measurements, err := q.Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			for _, g := range iris.TableGroupsForMeasurement(m) {
				sourceNames = append(sourceNames, g.Results.TableName)
			}
		}
		if len(sourceNames) == 0 {
			return fmt.Errorf("no results tables found in range %s to %s", fromStr, toStr)
		}
	}

	// ── Pre-scan all tables ───────────────────────────────────────────────────
	fmt.Printf("found %d table(s)   policy: %s\n", len(sourceNames), policy)
	tables := make([]tableInfo, 0, len(sourceNames))
	totalChunks := int64(0)
	for _, name := range sourceNames {
		total, err := countSourceRows(irisClient, name)
		if err != nil {
			return fmt.Errorf("failed to count rows in %s: %w", name, err)
		}
		chunks := (total + chunkSize - 1) / chunkSize
		if chunks == 0 {
			chunks = 1
		}
		tables = append(tables, tableInfo{name: name, total: total, chunks: chunks})
		totalChunks += chunks
	}
	fmt.Printf("total: %d chunks across %d table(s)\n", totalChunks, len(tables))

	// ── Fetch and write ───────────────────────────────────────────────────────
	var ewmaRate float64
	globalChunk := int64(0)

	for i, t := range tables {
		tablePolicy := policy
		if i > 0 {
			tablePolicy = store.PolicyAppend
		}

		fmt.Printf("[%d/%d] %s   %s rows   %d chunks\n",
			i+1, len(tables), t.name, store.FormatCount(t.total), t.chunks)

		for c := int64(0); c < t.chunks; c++ {
			offset := c * chunkSize
			chunkPolicy := store.PolicyAppend
			if c == 0 {
				chunkPolicy = tablePolicy
			}

			globalChunk++
			chunkRows := int64(chunkSize)
			if remaining := t.total - offset; remaining < chunkRows {
				chunkRows = remaining
			}

			sql := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", t.name, chunkSize, offset)
			rows, err := irisClient.Query().Select(sql).Json()
			if err != nil {
				return fmt.Errorf("[%d/%d] chunk %d: failed to query: %w", i+1, len(tables), c+1, err)
			}

			start := time.Now()
			if err := s.Put(chunkPolicy, dest, schema, rows); err != nil {
				return fmt.Errorf("[%d/%d] chunk %d: failed to write: %w", i+1, len(tables), c+1, err)
			}
			elapsed := time.Since(start)

			// Update EWMA rate (rows/sec).
			currentRate := float64(chunkRows) / elapsed.Seconds()
			if ewmaRate == 0 {
				ewmaRate = currentRate
			} else {
				ewmaRate = ewmaAlpha*currentRate + (1-ewmaAlpha)*ewmaRate
			}

			// Compute ETA from remaining chunks globally.
			remainingChunks := totalChunks - globalChunk
			var eta string
			if ewmaRate > 0 && remainingChunks > 0 {
				remainingSec := float64(remainingChunks) * float64(chunkSize) / ewmaRate
				remaining := time.Duration(remainingSec) * time.Second
				eta = fmt.Sprintf("%s (in ~%s)",
					time.Now().Add(remaining).Format("Jan 2, 3:04pm"),
					remaining.Round(time.Second),
				)
			} else {
				eta = "done"
			}

			fmt.Printf("      chunk %d/%d/%d   |   %s   |   %.0f rows/s   |   %s\n",
				c+1, globalChunk, totalChunks,
				elapsed.Round(time.Second),
				ewmaRate,
				eta,
			)
		}
	}

	return nil
}

// countSourceRows queries the row count of a source table on Iris.
func countSourceRows(client *iris.IrisClient, sourceTable string) (int64, error) {
	r, err := client.Query().
		Select(fmt.Sprintf("SELECT count() AS count FROM %s", sourceTable)).
		Json()
	if err != nil {
		return 0, err
	}
	defer r.Close()

	reader, err := decompressIfNeeded(r)
	if err != nil {
		return 0, fmt.Errorf("failed to decompress count response: %w", err)
	}

	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(reader).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}
	return result.Count, nil
}

// decompressIfNeeded detects gzip magic bytes and wraps the reader if needed.
func decompressIfNeeded(r io.ReadCloser) (io.Reader, error) {
	buf := make([]byte, 2)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	peeked := io.MultiReader(bytes.NewReader(buf[:n]), r)
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(peeked)
		if err != nil {
			return nil, err
		}
		return gz, nil
	}
	return peeked, nil
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
