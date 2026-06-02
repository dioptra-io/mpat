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

const chunkSize = 5_000_000 // 5M rows per chunk

func main() {
	var (
		policy      string
		tableFlag   string
		measurement string
		from        string
		to          string
		state       string
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
				tableFlag, measurement, from, to, state,
			)
		},
	}

	irisResultsCmd.Flags().StringVar(&policy, "policy", "fail", "Write policy: replace, truncate, fail, append")
	irisResultsCmd.Flags().StringVar(&tableFlag, "table", "", "Source table name (mode 1)")
	irisResultsCmd.Flags().StringVar(&measurement, "measurement", "", "Measurement UUID (mode 2)")
	irisResultsCmd.Flags().StringVar(&from, "from", "", "Start date, RFC3339 (mode 3)")
	irisResultsCmd.Flags().StringVar(&to, "to", "", "End date, RFC3339 (mode 3)")
	irisResultsCmd.Flags().StringVar(&state, "state", "finished", "Measurement state filter for mode 3 (default: finished)")

	fetchCmd.AddCommand(irisResultsCmd)
	rootCmd.AddCommand(fetchCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runIrisResults(destTable string, policy store.Policy, tableFlag, measurement, fromStr, toStr, stateStr string) error {
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
	var sourceTables []string

	switch {
	case tableFlag != "":
		sourceTables = []string{tableFlag}

	case measurement != "":
		measurements, err := irisClient.Measurements().Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			if m.UUID == measurement {
				for _, g := range iris.TableGroupsForMeasurement(m) {
					sourceTables = append(sourceTables, g.Results.TableName)
				}
				break
			}
		}
		if len(sourceTables) == 0 {
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
		measurements, err := q.Fetch()
		if err != nil {
			return fmt.Errorf("failed to fetch measurements: %w", err)
		}
		for _, m := range measurements {
			for _, g := range iris.TableGroupsForMeasurement(m) {
				sourceTables = append(sourceTables, g.Results.TableName)
			}
		}
		if len(sourceTables) == 0 {
			return fmt.Errorf("no results tables found in range %s to %s", fromStr, toStr)
		}
	}

	// ── Fetch and write ───────────────────────────────────────────────────────
	for _, sourceTable := range sourceTables {
		if err := fetchAndWrite(irisClient, s, sourceTable, dest, schema, policy); err != nil {
			return fmt.Errorf("failed to write table %s: %w", sourceTable, err)
		}
	}

	return nil
}

// fetchAndWrite fetches a source table in chunks and writes to dest.
// The first chunk applies the policy; subsequent chunks always append.
func fetchAndWrite(irisClient *iris.IrisClient, s *store.Store, sourceTable string, dest store.DestTable, schema string, policy store.Policy) error {
	total, err := countSourceRows(irisClient, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	fmt.Printf("fetching %s → %s.%s (policy: %s, total: %s rows)\n",
		sourceTable, dest.Database, dest.Table, policy, store.FormatCount(total))

	chunks := (total + chunkSize - 1) / chunkSize
	if chunks == 0 {
		chunks = 1
	}

	for i := int64(0); i < chunks; i++ {
		offset := i * chunkSize
		chunkPolicy := store.PolicyAppend
		if i == 0 {
			chunkPolicy = policy
		}

		remaining := total - offset
		thisChunk := int64(chunkSize)
		if remaining < thisChunk {
			thisChunk = remaining
		}

		fmt.Printf("  chunk %d/%d\n", i+1, chunks)

		sql := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", sourceTable, chunkSize, offset)
		rows, err := irisClient.Query().Select(sql).Json()
		if err != nil {
			return fmt.Errorf("chunk %d: failed to query: %w", i+1, err)
		}

		if err := s.Put(chunkPolicy, dest, schema, rows, thisChunk); err != nil {
			return fmt.Errorf("chunk %d: failed to write: %w", i+1, err)
		}
	}

	fmt.Printf("done: %s\n", sourceTable)
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
