package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	apiv1 "dioptra-io/ufuk-research/pkg/api/v1"
)

// Database client for low level operations such as running queries etc.
type DBClient interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error)
	Driver() driver.Driver
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

// Iris client implements some of the medhod the Iris API provides. Normally
// this should include all but due to time constraints the functionality is
// limited.
type IrisClient interface {
	// XXX empty for now
}

// For interacting with Ark IPv4 dataset. For more info about the dataset:
// https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily
type ArkClient interface {
	// Get the cycles of thise given dates.
	GetCyclesFor(ctx context.Context, dates []time.Time) ([]apiv1.ArkCycle, error)

	// Get the cycles of thise given dates.
	GetCyclesBetween(ctx context.Context, after, before time.Time) ([]apiv1.ArkCycle, error)

	// Get the wart file
	GetWartfile(ctx context.Context, cycle apiv1.ArkCycle) ([]apiv1.ArkWartFile, error)
}
