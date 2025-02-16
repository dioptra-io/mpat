package copyiristables

import (
	"time"

	"github.com/chigopher/pathlib"
)

// This is the config file that the database uses.
type DatabaseConfig struct {
	User      string
	Password  string
	Database  string
	Host      string
	ChunkSize int
	TableType string
}

func (c *DatabaseConfig) GetTablesOfMeasUUID(measUUID string) ([]string, error) {
	time.Sleep(time.Second)
	return []string{}, nil
}

func (c *DatabaseConfig) CreateResultsTable() error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) DownloadResultsTable(file *pathlib.Path) error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) UploadResultsTable(file *pathlib.Path) error {
	time.Sleep(time.Second)
	return nil
}

func (c *DatabaseConfig) GetSizeOfResultsTable(tableName string) (int, error) {
	time.Sleep(time.Second)
	return 2, nil
}

func (c *DatabaseConfig) TableExists(tableName string) (bool, error) {
	time.Sleep(time.Second)
	return false, nil
}
