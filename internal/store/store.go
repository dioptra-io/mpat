package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	_ "embed"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	DefaultDatabase = "mpat"
)

// StorePolicy controls how the store behaves when inserting into a table that already contains data.
type StorePolicy string

const (
	// StorePolicyReplace drops the table and recreates it before inserting.
	StorePolicyReplace StorePolicy = "replace"
	// StorePolicyTruncate truncates the table before inserting.
	StorePolicyTruncate StorePolicy = "truncate"
	// StorePolicyFail returns an error if the table contains any rows before inserting.
	StorePolicyFail StorePolicy = "fail"
	// StorePolicyAppend inserts without any prior checks or modifications.
	StorePolicyAppend StorePolicy = "append"
)

// DatabaseTable identifies a table within a specific database.
type DatabaseTable struct {
	Database string
	Table    string
}

type Store struct {
	conn       clickhouse.Conn
	config     *StoreConfig
	httpClient *http.Client
	httpHost   string
}

type StoreConfig struct {
	Host     string
	Username string
	Password string
	Database string // defaults to "mpat"
}

func (c *StoreConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("store: host is required")
	}
	if c.Username == "" {
		return fmt.Errorf("store: username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("store: password is required")
	}
	if c.Database == "" {
		c.Database = DefaultDatabase
	}
	return nil
}

func ConfigFromDSN(dsn string) (*StoreConfig, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: failed to parse DSN: %w", err)
	}
	host := ""
	if len(opts.Addr) > 0 {
		host = opts.Addr[0]
	}
	return &StoreConfig{
		Host:     host,
		Username: opts.Auth.Username,
		Password: opts.Auth.Password,
		Database: opts.Auth.Database,
	}, nil
}

func NewStore(config *StoreConfig) (*Store, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("store: host is required")
	}
	if config.Database == "" {
		config.Database = DefaultDatabase
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Host},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("store: failed to open connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("store: failed to ping clickhouse: %w", err)
	}

	// Derive HTTP host: same hostname, port 8123.
	hostname, _, err := net.SplitHostPort(config.Host)
	if err != nil {
		hostname = config.Host
	}
	httpHost := hostname + ":8123"

	return &Store{
		conn:       conn,
		config:     config,
		httpClient: &http.Client{},
		httpHost:   httpHost,
	}, nil
}

func (s *Store) HandlePolicy(policy StorePolicy, dest DatabaseTable, schema string) error {
	ctx := context.Background()
	qualified := fmt.Sprintf("%s.%s", dest.Database, dest.Table)

	switch policy {
	case StorePolicyReplace:
		if err := s.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified)); err != nil {
			return fmt.Errorf("store: replace: failed to drop table: %w", err)
		}
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: replace: failed to create table: %w", err)
		}

	case StorePolicyTruncate:
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: truncate: failed to create table if not exists: %w", err)
		}
		count, err := s.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("store: truncate: failed to count rows: %w", err)
		}
		if count > 0 {
			if err := s.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualified)); err != nil {
				return fmt.Errorf("store: truncate: failed to truncate table: %w", err)
			}
		}

	case StorePolicyFail:
		count, err := s.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("store: fail: failed to count rows: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("store: fail: destination table %s is not empty (%d rows)", qualified, count)
		}
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: fail: failed to create table if not exists: %w", err)
		}

	case StorePolicyAppend:
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: append: failed to create table if not exists: %w", err)
		}

	default:
		return fmt.Errorf("store: unknown policy %q", policy)
	}
	return nil
}

// RowCount returns the number of rows in dest, or 0 if the table does not exist.
func (s *Store) RowCount(ctx context.Context, dest DatabaseTable) (uint64, error) {
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

// Exec runs a DDL or DML statement via the native driver.
func (s *Store) Exec(ctx context.Context, query string, args ...any) error {
	return s.conn.Exec(ctx, query, args...)
}

// InsertJSONL streams rows directly into ClickHouse via HTTP POST.
// If the stream is gzip-compressed, it is decompressed transparently before sending.
// The format is required to be JSONEachRow
func (s *Store) InsertJSONL(dest DatabaseTable, rows io.Reader) error {
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", dest.Database, dest.Table)

	params := url.Values{}
	params.Set("query", query)
	params.Set("database", dest.Database)
	params.Set("max_execution_time", "3600")
	params.Set("receive_timeout", "3600")
	params.Set("send_timeout", "3600")

	u := fmt.Sprintf("http://%s/?%s", s.httpHost, params.Encode())

	// Peek at the first two bytes to detect gzip magic number (0x1f 0x8b).
	buf := make([]byte, 2)
	n, err := io.ReadFull(rows, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("store: failed to peek stream: %w", err)
	}
	peeked := io.MultiReader(bytes.NewReader(buf[:n]), rows)

	var body = peeked
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(peeked)
		if err != nil {
			return fmt.Errorf("store: failed to create gzip reader: %w", err)
		}
		defer func() { _ = gz.Close() }()
		body = gz
	}

	req, err := http.NewRequest(http.MethodPost, u, body)
	if err != nil {
		return fmt.Errorf("store: failed to build insert request: %w", err)
	}
	req.SetBasicAuth(s.config.Username, s.config.Password)
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
