package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"time"
)

// I need to get all of thise in one interface becuase go std lib sucks
type ClickHouseSQLAdapter interface {
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

// This clickhouse http adapter is used for processing bulk data which we don't need to
// invoke Scan for each row.
type ClickHouseHTTPAdapter interface {
	// If there is a connection that needs to be closed this should be invoked.
	Close() error

	// Download function runs a query and returns the readcloser from it. This
	// is can be read after the function call to stream the data.
	Download(query string) (io.ReadCloser, error)

	// Upload function gets a readcloser to send the data. Note that it does not
	// closes the r. Since it also does a request, it also returns a readcloser.
	Upload(query string, r io.Reader) (io.ReadCloser, error)
}

type ArkHTTPAdapter interface {
	Download()
}

// This is the main interface for iris. For now only the clickhouse stuff is implemented.
// But in the future the API adapter, etc... will also be added there.
type IrisClient interface {
	// If not yet created opens a connection to clickhouse using sql.Open and returns the
	// instance. If the reOpenIfExists is set to true it closes the exsiting connection and
	// reopens another one.
	ClickHouseSQLAdapter(reOpenIfExists bool) (ClickHouseSQLAdapter, error)

	// Similar to the `ClickHouseSQLAdapter` however, for more efficient data transfer it
	// uses http requests.
	ClickHouseHTTPAdapter(reOpenIfExists bool) (ClickHouseHTTPAdapter, error)
}

// This is the converter interface that takes a io.Reader and outputs another one while
// performing the operation.
type Converter interface {
	Convert(r io.Reader) (io.Reader, error)
}

// This is the converter interface that takes a io.Reader and outputs another one while
// performing the operation. Different from the Closer interface is that the resulting
// is a io.ReadCloser interface which needs to be closed when finished.
type ConvertCloser interface {
	Convert(r io.Reader) (io.ReadCloser, error)
}

// This is also a converter but instead of returning a io.Reader it retuns a generic
// readonly chan.
type ConverterChan[T any] interface {
	// Here we observe one interesting behavior, since there are two chans there is a possibility
	// that one is closed and other is not, in a select statement. Thus the caller should check both
	// channels. Here is an exmaple:
	//
	// r := strings.NewReader(str)
	//
	// objectsCh, errCh := converter.Convert(r)
	// continueLoop := true
	//
	//	for continueLoop {
	//		select {
	//		case rec, ok := <-objectsCh:
	//			if ok {
	//              // Do something with rec.
	//			} else {
	//				continueLoop = false
	//			}
	//		case err, ok := <-errCh:
	//			if ok {
	//				panic(err)
	//			} else {
	//				continueLoop = false
	//			}
	//		}
	//	}
	//
	// We cannot guarantee if the caller would be on the first or the second closed channel
	// after closing the channels.
	Convert(r io.Reader) (<-chan T, <-chan error)
}
