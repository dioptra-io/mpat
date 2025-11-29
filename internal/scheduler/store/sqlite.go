package store

import (
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SQLiteStore struct {
	db *gorm.DB
}

// NewSQLiteStore creates a new SQLite store with the given database file path.
// Use ":memory:" for an in-memory database.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Auto-migrate the schema
	if err := db.AutoMigrate(&api.Command{}); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// Close closes the database connection
func (s *SQLiteStore) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// CreateCommand creates a new command with the given payload, it also creates the Tasks.
func (s *SQLiteStore) CreateCommand(payload string) (*api.Command, error) {
	cmd := &api.Command{
		Payload:   payload,
		Tasks:     make(map[api.NamedVersion]*api.Task),
		CreatedAt: time.Now(),
	}

	if err := s.db.Create(cmd).Error; err != nil {
		return nil, fmt.Errorf("failed to create command: %w", err)
	}

	return cmd, nil
}

// LoadCommand loads the command with the commandID also populates the Task fields.
func (s *SQLiteStore) LoadCommand(commandID uint) (*api.Command, error) {
	var cmd api.Command
	result := s.db.First(&cmd, commandID)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("command with ID %d does not exist", commandID)
		}
		return nil, fmt.Errorf("failed to load command: %w", result.Error)
	}

	return &cmd, nil
}

// SaveCommand saves the command and its tasks.
func (s *SQLiteStore) SaveCommand(c *api.Command) error {
	result := s.db.Save(c)
	if result.Error != nil {
		return fmt.Errorf("failed to save command: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("command with ID %d does not exist", c.ID)
	}

	return nil
}

// GetAllCommandIDs gets all the commandIDs of the commands.
func (s *SQLiteStore) GetAllCommandIDs() ([]uint, error) {
	var ids []uint
	if err := s.db.Model(&api.Command{}).Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("failed to get command IDs: %w", err)
	}

	return ids, nil
}
