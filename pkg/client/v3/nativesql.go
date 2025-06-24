package v3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type NativeSQLClient struct {
	clickhouse.Conn

	Database string
}

func NewNativeSQLClient(dsn string) (*NativeSQLClient, error) {
	parsedUrl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	host := parsedUrl.Host
	if host == "" {
		host = "localhost:9000"
	}

	username := parsedUrl.User.Username()
	password, _ := parsedUrl.User.Password()

	dbname := parsedUrl.Query().Get("database")
	if dbname == "" {
		dbname = "default"
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: dbname,
			Username: username,
			Password: password,
		},
		Protocol: clickhouse.HTTP,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionNone,
		},
		// Settings: clickhouse.Settings{
		// 	"send_timeout":    360000 * time.Hour,
		// 	"receive_timeout": 360000 * time.Hour,
		// },
	})
	if err != nil {
		return nil, err
	}

	return &NativeSQLClient{
		Conn:     conn,
		Database: dbname,
	}, nil
}

func NewNativeSQLClientWithPing(dsn string) (*NativeSQLClient, error) {
	client, err := NewNativeSQLClient(dsn)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.TODO()); err != nil {
		return nil, err
	}
	return client, nil
}

func (a *NativeSQLClient) TableSize(ctx context.Context, tableName string) (uint64, error) {
	query := `
SELECT count() AS table_size 
FROM system.tables 
WHERE database = '%s' AND name = '%s'
;` // end of the query

	formattedQuery := fmt.Sprintf(
		query,
		a.Database,
		tableName,
	)

	var tableSize uint64
	if err := a.QueryRow(ctx, formattedQuery).Scan(&tableSize); err != nil {
		return 0, err
	}
	return tableSize, nil
}
