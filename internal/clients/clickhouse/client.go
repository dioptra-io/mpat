package clickhouse

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/ClickHouse/clickhouse-go/v2" // Import ClickHouse driver
)

// ClickHouseClient defines the interface for interacting with ClickHouse
type ClickHouseClient interface {
	// ExecuteQuery executes a SQL query and streams the response to the provided writer
	// The caller is responsible for closing the writer
	ExecuteQuery(ctx context.Context, query string, writer io.Writer) error

	// QueryStream executes a SQL query and returns the result as a readable stream
	// The caller is responsible for closing the returned io.ReadCloser
	QueryStream(ctx context.Context, query string) (io.ReadCloser, error)

	// QueryString executes a SQL query and returns the complete result as a string
	// This loads the entire response into memory - use QueryStream for large results
	QueryString(ctx context.Context, query string) (string, error)

	// Execute runs a SQL statement (like INSERT, CREATE, DROP) that doesn't return data
	Execute(ctx context.Context, statement string) error

	// Ping checks if the ClickHouse server is reachable and responding
	Ping(ctx context.Context) error
}

type clickHouseClient struct {
	db *sql.DB
}

var _ ClickHouseClient = (*clickHouseClient)(nil)

func NewClickHouseClient(host string, port int, database, username, password string) (ClickHouseClient, error) {
	// Build connection string
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		username,
		password,
		host,
		port,
		database)

	// Open connection
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &clickHouseClient{
		db: db,
	}, nil
}

// ExecuteQuery executes a SQL query and streams the response to the provided writer
// The caller is responsible for closing the writer
func (c *clickHouseClient) ExecuteQuery(ctx context.Context, query string, writer io.Writer) error {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare value containers
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Write header
	for i, col := range columns {
		if i > 0 {
			fmt.Fprint(writer, "\t")
		}
		fmt.Fprint(writer, col)
	}
	fmt.Fprintln(writer)

	// Write rows
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			if i > 0 {
				fmt.Fprint(writer, "\t")
			}
			fmt.Fprintf(writer, "%v", val)
		}
		fmt.Fprintln(writer)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	return nil
}

// QueryStream executes a SQL query and returns the result as a readable stream
// The caller is responsible for closing the returned io.ReadCloser
func (c *clickHouseClient) QueryStream(ctx context.Context, query string) (io.ReadCloser, error) {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Create a pipe to stream data
	pr, pw := io.Pipe()

	// Start a goroutine to write query results to the pipe
	go func() {
		defer rows.Close()
		defer pw.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			pw.CloseWithError(fmt.Errorf("failed to get columns: %w", err))
			return
		}

		// Prepare value containers
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Write header
		for i, col := range columns {
			if i > 0 {
				fmt.Fprint(pw, "\t")
			}
			fmt.Fprint(pw, col)
		}
		fmt.Fprintln(pw)

		// Write rows
		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				pw.CloseWithError(fmt.Errorf("failed to scan row: %w", err))
				return
			}

			for i, val := range values {
				if i > 0 {
					fmt.Fprint(pw, "\t")
				}
				fmt.Fprintf(pw, "%v", val)
			}
			fmt.Fprintln(pw)
		}

		if err := rows.Err(); err != nil {
			pw.CloseWithError(fmt.Errorf("row iteration error: %w", err))
			return
		}
	}()

	return pr, nil
}

// QueryString executes a SQL query and returns the complete result as a string
// This loads the entire response into memory - use QueryStream for large results
func (c *clickHouseClient) QueryString(ctx context.Context, query string) (string, error) {
	// Use QueryStream to get the data
	reader, err := c.QueryStream(ctx, query)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	// Read entire stream into memory
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	if err != nil {
		return "", fmt.Errorf("failed to read query result: %w", err)
	}

	return buf.String(), nil
}

// Execute runs a SQL statement (like INSERT, CREATE, DROP) that doesn't return data
func (c *clickHouseClient) Execute(ctx context.Context, statement string) error {
	_, err := c.db.ExecContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// Ping checks if the ClickHouse server is reachable and responding
func (c *clickHouseClient) Ping(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}
	return nil
}
