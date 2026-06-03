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
	"github.com/dioptra-io/ufuk-research/internal/schema"
)

const (
	DefaultDatabase = "mpat"
)

// PreparationPolicy controls how the store behaves when inserting into a table that already contains data.
type PreparationPolicy string

const (
	// PreparationPolicyReplace drops the table and recreates it before inserting.
	PreparationPolicyReplace PreparationPolicy = "replace"
	// PreparationPolicyTruncate truncates the table before inserting.
	PreparationPolicyTruncate PreparationPolicy = "truncate"
	// PreparationPolicyFail returns an error if the table contains any rows before inserting.
	PreparationPolicyFail PreparationPolicy = "fail"
	// PreparationPolicyAppend inserts without any prior checks or modifications.
	PreparationPolicyAppend PreparationPolicy = "append"
)

// DatabaseTable identifies a table within a specific database.
type DatabaseTable struct {
	Database string
	Table    string
}

type Store struct {
	clickhouse.Conn
	config     *StoreConfig
	httpClient *http.Client
	httpHost   string
}

type StoreConfig struct {
	Host     string
	Username string
	Password string
	Database string // defaults to DefaultDatabase
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
		Conn:       conn,
		config:     config,
		httpClient: &http.Client{},
		httpHost:   httpHost,
	}, nil
}

// PrepareTable prepares the destination table according to the given write policy
// before any data is inserted. It must be called before writing rows to dest.
// The schema DDL is rendered from the provided Schema using the destination
// database and table name.
//
// The following policies are supported:
//
//   - StorePolicyReplace:  Drops the destination table if it exists, then recreates it
//     using the rendered schema DDL. All existing data is lost.
//
//   - StorePolicyTruncate: Creates the destination table if it does not exist, then
//     truncates it if it contains any rows. The table structure is preserved.
//
//   - StorePolicyFail:     Fails with an error if the destination table already contains
//     rows. If the table is empty or does not exist, it is created using the rendered
//     schema DDL.
//
//   - StorePolicyAppend:   Creates the destination table if it does not exist, then
//     leaves any existing rows intact. New rows will be appended on insert.
func (s *Store) PrepareTable(ctx context.Context, writePolicy PreparationPolicy, dest DatabaseTable, schemaInterface schema.Schema) error {
	schema := schemaInterface.DDL(dest.Database, dest.Table)
	qualified := fmt.Sprintf("%s.%s", dest.Database, dest.Table)

	switch writePolicy {
	case PreparationPolicyReplace:
		if err := s.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified)); err != nil {
			return fmt.Errorf("store: replace: failed to drop table: %w", err)
		}
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: replace: failed to create table: %w", err)
		}

	case PreparationPolicyTruncate:
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

	case PreparationPolicyFail:
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

	case PreparationPolicyAppend:
		if err := s.Exec(ctx, schema); err != nil {
			return fmt.Errorf("store: append: failed to create table if not exists: %w", err)
		}

	default:
		return fmt.Errorf("store: unknown policy %q", writePolicy)
	}
	return nil
}

// TableSchema returns the schema of the given table as a DynamicSchema,
// or nil if the table does not exist.
func (s *Store) TableSchema(ctx context.Context, dest DatabaseTable) (*schema.DynamicSchema, error) {
	var exists uint64
	err := s.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?",
		dest.Database, dest.Table,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("store: failed to check table existence: %w", err)
	}
	if exists == 0 {
		return nil, nil
	}

	var ddl string
	err = s.QueryRow(ctx,
		"SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?",
		dest.Database, dest.Table,
	).Scan(&ddl)
	if err != nil {
		return nil, fmt.Errorf("store: failed to get table DDL: %w", err)
	}

	schema, err := schema.NewDynamicSchema(ddl)
	if err != nil {
		return nil, fmt.Errorf("store: failed to parse table schema: %w", err)
	}
	return schema, nil
}

// RowCount returns the number of rows in dest, or 0 if the table does not exist.
func (s *Store) RowCount(ctx context.Context, dest DatabaseTable) (uint64, error) {
	var exists uint64
	err := s.QueryRow(ctx,
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
	err = s.QueryRow(ctx,
		fmt.Sprintf("SELECT count() FROM %s.%s", dest.Database, dest.Table),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("store: failed to count rows: %w", err)
	}
	return count, nil
}

// InsertJSONL streams rows directly into ClickHouse via HTTP POST.
// If the stream is gzip-compressed, it is decompressed transparently before sending.
// The input format is required to be JSONEachRow.
func (s *Store) InsertJSONL(dest DatabaseTable, rows io.Reader) error {
	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", dest.Database, dest.Table)

	params := url.Values{}
	params.Set("query", query)
	params.Set("database", dest.Database)
	params.Set("max_execution_time", "3600") // hardcoded for now
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

// func renderTemplate(name, tmpl string, data any) (string, error) {
// 	t, err := template.New(name).Parse(tmpl)
// 	if err != nil {
// 		return "", err
// 	}
// 	var buf bytes.Buffer
// 	if err := t.Execute(&buf, data); err != nil {
// 		return "", err
// 	}
// 	return buf.String(), nil
// }
