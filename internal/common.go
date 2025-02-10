package internal

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"
)

func NewConnection() (clickhouse.Conn, error) {
	database := viper.GetString("database")
	user := viper.GetString("user")
	password := viper.GetString("password")
	host := viper.GetString("host")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		ConnMaxLifetime: time.Hour * 24 * 365,
	})

	return conn, err
}
