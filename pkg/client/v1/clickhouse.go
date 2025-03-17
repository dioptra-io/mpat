package v1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"dioptra-io/ufuk-research/pkg/client"
)

type clickHouseClient struct {
	client.DBClient

	dsn  string
	conn *sql.DB
}

var _ client.DBClient = (*clickHouseClient)(nil)

func NewClickHouseClient(dsn string) (client.DBClient, error) {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	return &clickHouseClient{
		dsn:  dsn,
		conn: conn,
	}, nil
}

func (c *clickHouseClient) Begin() (*sql.Tx, error) {
	return c.conn.Begin()
}

func (c *clickHouseClient) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.conn.BeginTx(ctx, opts)
}

func (c *clickHouseClient) Close() error {
	return c.conn.Close()
}

func (c *clickHouseClient) Conn(ctx context.Context) (*sql.Conn, error) {
	return c.conn.Conn(ctx)
}

func (c *clickHouseClient) Driver() driver.Driver {
	return c.conn.Driver()
}

func (c *clickHouseClient) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.Exec(query, args...)
}

func (c *clickHouseClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

func (c *clickHouseClient) Ping() error {
	return c.conn.Ping()
}

func (c *clickHouseClient) PingContext(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

func (c *clickHouseClient) Prepare(query string) (*sql.Stmt, error) {
	return c.conn.Prepare(query)
}

func (c *clickHouseClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.conn.PrepareContext(ctx, query)
}

func (c *clickHouseClient) Query(query string, args ...any) (*sql.Rows, error) {
	return c.conn.Query(query, args...)
}

func (c *clickHouseClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

func (c *clickHouseClient) QueryRow(query string, args ...any) *sql.Row {
	return c.conn.QueryRow(query, args...)
}

func (c *clickHouseClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}

func (c *clickHouseClient) SetConnMaxIdleTime(d time.Duration) {
	c.conn.SetConnMaxIdleTime(d)
}

func (c *clickHouseClient) SetConnMaxLifetime(d time.Duration) {
	c.conn.SetConnMaxLifetime(d)
}

func (c *clickHouseClient) SetMaxIdleConns(n int) {
	c.conn.SetMaxIdleConns(n)
}

func (c *clickHouseClient) SetMaxOpenConns(n int) {
	c.conn.SetMaxOpenConns(n)
}

func (c *clickHouseClient) Stats() sql.DBStats {
	return c.conn.Stats()
}
