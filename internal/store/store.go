package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"text/template"
	"time"

	_ "embed"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	defaultDatabase = "mpat"
)

//go:embed sql/results.sql
var resultsDDLTemplate string

// ── Config ───────────────────────────────────────────────────────────────────

type StoreConfig struct {
	ConnectionString string
	Database         string // defaults to "mpat"
}

// ── DestTable ─────────────────────────────────────────────────────────────────

type DestTable struct {
	Database string
	Table    string
}

// ── Policy ───────────────────────────────────────────────────────────────────

type Policy string

const (
	PolicyReplace  Policy = "replace"
	PolicyTruncate Policy = "truncate"
	PolicyFail     Policy = "fail"
	PolicyAppend   Policy = "append"
)

// ── insertFormat ─────────────────────────────────────────────────────────────

type insertFormat string

const (
	FormatJSON      insertFormat = "JSONEachRow"
	FormatRowBinary insertFormat = "RowBinaryWithNamesAndTypes"
)

// ── Store ────────────────────────────────────────────────────────────────────

type Store struct {
	conn       clickhouse.Conn
	config     StoreConfig
	httpClient *http.Client
	httpDSN    *dsn
}

// dsn holds parsed connection info for HTTP inserts.
type dsn struct {
	host     string
	database string
	username string
	password string
}

func NewStore(cfg StoreConfig) (*Store, error) {
	if cfg.ConnectionString == "" {
		return nil, fmt.Errorf("store: connection string is required")
	}
	if cfg.Database == "" {
		cfg.Database = defaultDatabase
	}

	opts, err := clickhouse.ParseDSN(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("store: failed to parse connection string: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("store: failed to open connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("store: failed to ping clickhouse: %w", err)
	}

	// Parse HTTP DSN for streaming inserts (port 8123 instead of 9000).
	u, err := url.Parse(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("store: failed to parse DSN for HTTP: %w", err)
	}
	host := u.Hostname() + ":8123"
	username := u.User.Username()
	password, _ := u.User.Password()
	database := cfg.Database
	if u.Path != "" && u.Path != "/" {
		database = u.Path[1:]
	}

	return &Store{
		conn:       conn,
		config:     cfg,
		httpClient: &http.Client{},
		httpDSN: &dsn{
			host:     host,
			database: database,
			username: username,
			password: password,
		},
	}, nil
}

// ResultsDDL renders the results.sql template for the given destination table.
func ResultsDDL(dest DestTable) (string, error) {
	tmpl, err := template.New("results").Parse(resultsDDLTemplate)
	if err != nil {
		return "", fmt.Errorf("store: failed to parse results DDL template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, dest); err != nil {
		return "", fmt.Errorf("store: failed to render results DDL template: %w", err)
	}

	return buf.String(), nil
}

// ── Put ──────────────────────────────────────────────────────────────────────

// Put writes a JSONEachRow stream into dest according to the given policy.
// total is the expected number of rows (used for progress reporting).
func (s *Store) Put(policy Policy, dest DestTable, schema string, rows io.ReadCloser, total int64, chunkInfo string) error {
	return s.put(policy, dest, schema, rows, FormatJSON, total, chunkInfo)
}

// PutRowBinary writes a RowBinaryWithNamesAndTypes stream into dest according to the given policy.
// More efficient than Put for large tables — no JSON parsing overhead on either side.
// total is the expected number of rows (used for progress reporting).
func (s *Store) PutRowBinary(policy Policy, dest DestTable, schema string, rows io.ReadCloser, total int64, chunkInfo string) error {
	return s.put(policy, dest, schema, rows, FormatRowBinary, total, chunkInfo)
}

// put is the shared implementation for Put and PutRowBinary.
func (s *Store) put(policy Policy, dest DestTable, schema string, rows io.ReadCloser, format insertFormat, total int64, chunkInfo string) error {
	defer rows.Close()

	ctx := context.Background()
	qualified := fmt.Sprintf("%s.%s", dest.Database, dest.Table)

	switch policy {
	case PolicyReplace:
		if err := s.exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified)); err != nil {
			return fmt.Errorf("store: replace: failed to drop table: %w", err)
		}
		if err := s.exec(ctx, schema); err != nil {
			return fmt.Errorf("store: replace: failed to create table: %w", err)
		}

	case PolicyTruncate:
		if err := s.exec(ctx, schema); err != nil {
			return fmt.Errorf("store: truncate: failed to create table if not exists: %w", err)
		}
		count, err := s.rowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("store: truncate: failed to count rows: %w", err)
		}
		if count > 0 {
			if err := s.exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualified)); err != nil {
				return fmt.Errorf("store: truncate: failed to truncate table: %w", err)
			}
		}

	case PolicyFail:
		count, err := s.rowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("store: fail: failed to count rows: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("store: fail: destination table %s is not empty (%d rows)", qualified, count)
		}
		if err := s.exec(ctx, schema); err != nil {
			return fmt.Errorf("store: fail: failed to create table if not exists: %w", err)
		}

	case PolicyAppend:
		if err := s.exec(ctx, schema); err != nil {
			return fmt.Errorf("store: append: failed to create table if not exists: %w", err)
		}

	default:
		return fmt.Errorf("store: unknown policy %q", policy)
	}

	return s.insert(dest, rows, format, total, chunkInfo)
}

// ── Newline counting reader ───────────────────────────────────────────────────

// rowCountingReader wraps an io.Reader and counts newlines as they pass through.
// Each newline in JSONEachRow corresponds to one row.
type rowCountingReader struct {
	r       io.Reader
	counter *atomic.Int64
}

func (r *rowCountingReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	for i := 0; i < n; i++ {
		if p[i] == '\n' {
			r.counter.Add(1)
		}
	}
	return
}

// ── Progress reporting ────────────────────────────────────────────────────────

const ewmaAlpha = 0.2

// progressReporter reads from counter and prints stats every interval.
// Uses EWMA (alpha=0.2) to smooth the rows/sec estimate.
// Runs in a goroutine; stops when done is closed.
func progressReporter(counter *atomic.Int64, total int64, interval time.Duration, start time.Time, chunkInfo string, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		ewmaRate    float64
		lastFetched int64
	)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			fetched := counter.Load()
			delta := fetched - lastFetched
			lastFetched = fetched

			currentRate := float64(delta) / interval.Seconds()
			if ewmaRate == 0 {
				ewmaRate = currentRate
			} else {
				ewmaRate = ewmaAlpha*currentRate + (1-ewmaAlpha)*ewmaRate
			}

			printProgress(chunkInfo, fetched, total, ewmaRate, time.Since(start))
		}
	}
}

func printProgress(chunkInfo string, fetched, total int64, ewmaRate float64, elapsed time.Duration) {
	var (
		pct       float64
		remaining time.Duration
		eta       string
	)

	if total > 0 {
		pct = float64(fetched) / float64(total) * 100
	}
	if ewmaRate > 0 && total > fetched {
		remainingSec := float64(total-fetched) / ewmaRate
		remaining = time.Duration(remainingSec) * time.Second
		eta = time.Now().Add(remaining).Format("Jan 2, 3:04pm")
	} else {
		eta = "N/A"
	}

	fmt.Printf(
		"  %s | rows: %s / %s (%.1f%%) | %.0f rows/s | elapsed: %s | remaining: ~%s | ETA: %s\n",
		chunkInfo,
		FormatCount(fetched),
		FormatCount(total),
		pct,
		ewmaRate,
		elapsed.Round(time.Second),
		remaining.Round(time.Second),
		eta,
	)
}

// FormatCount formats an integer with thousands separators.
func FormatCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// rowCount returns the number of rows in dest, or 0 if the table does not exist.
func (s *Store) rowCount(ctx context.Context, dest DestTable) (uint64, error) {
	var exists uint64
	err := s.conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
		dest.Database, dest.Table,
	).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("store: failed to check table existence: %w", err)
	}
	if exists == 0 {
		return 0, nil
	}

	var count uint64
	err = s.conn.QueryRow(ctx,
		fmt.Sprintf("SELECT count() FROM %s.%s", dest.Database, dest.Table),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("store: failed to count rows: %w", err)
	}
	return count, nil
}

// exec runs a DDL or DML statement via the native driver.
func (s *Store) exec(ctx context.Context, query string) error {
	return s.conn.Exec(ctx, query)
}

// insert streams rows directly into ClickHouse via HTTP POST.
// If the stream is gzip-compressed, it is decompressed transparently before sending.
// Progress is tracked by counting newlines passing through the stream.
func (s *Store) insert(dest DestTable, rows io.Reader, format insertFormat, total int64, chunkInfo string) error {
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT %s", dest.Database, dest.Table, format)

	params := url.Values{}
	params.Set("query", query)
	params.Set("database", dest.Database)
	params.Set("max_execution_time", "3600")
	params.Set("receive_timeout", "3600")
	params.Set("send_timeout", "3600")

	u := fmt.Sprintf("http://%s/?%s", s.httpDSN.host, params.Encode())

	// Peek at the first two bytes to detect gzip magic number (0x1f 0x8b).
	buf := make([]byte, 2)
	n, err := io.ReadFull(rows, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("store: failed to peek stream: %w", err)
	}
	peeked := io.MultiReader(bytes.NewReader(buf[:n]), rows)

	var body io.Reader = peeked
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(peeked)
		if err != nil {
			return fmt.Errorf("store: failed to create gzip reader: %w", err)
		}
		defer gz.Close()
		body = gz
	}

	// Wrap body in a newline-counting reader for progress tracking.
	var counter atomic.Int64
	counted := &rowCountingReader{r: body, counter: &counter}

	// Start progress reporter goroutine if enabled.
	start := time.Now()
	if total > 0 {
		done := make(chan struct{})
		defer close(done)
		go progressReporter(&counter, total, 5*time.Second, start, chunkInfo, done)
	}

	req, err := http.NewRequest(http.MethodPost, u, counted)
	if err != nil {
		return fmt.Errorf("store: failed to build insert request: %w", err)
	}
	req.SetBasicAuth(s.httpDSN.username, s.httpDSN.password)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("store: insert request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("store: insert failed (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}
