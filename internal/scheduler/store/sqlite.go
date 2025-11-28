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
	if err := db.AutoMigrate(&api.Command{}, &api.Task{}); err != nil {
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

// CreateEmptyCommand creates and persists a new empty command.
func (s *SQLiteStore) CreateEmptyCommand() (*api.Command, error) {
	cmd := &api.Command{
		TaskIDs:   []uint{},
		CreatedAt: time.Now(),
	}

	if err := s.db.Create(cmd).Error; err != nil {
		return nil, fmt.Errorf("failed to create command: %w", err)
	}

	return cmd, nil
}

// CreateEmptyTask creates and persists a new empty task.
func (s *SQLiteStore) CreateEmptyTask() (*api.Task, error) {
	task := &api.Task{
		CreatedAt: time.Now(),
	}

	if err := s.db.Create(task).Error; err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	return task, nil
}

// UpdateCommand updates an existing command. Returns an error if it does not exist.
func (s *SQLiteStore) UpdateCommand(c *api.Command) error {
	result := s.db.Model(&api.Command{}).Where("id = ?", c.ID).Updates(c)
	if result.Error != nil {
		return fmt.Errorf("failed to update command: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("command with ID %d does not exist", c.ID)
	}
	return nil
}

// UpdateTask updates an existing task. Returns an error if it does not exist.
func (s *SQLiteStore) UpdateTask(t *api.Task) error {
	result := s.db.Model(&api.Task{}).Where("id = ?", t.ID).Updates(t)
	if result.Error != nil {
		return fmt.Errorf("failed to update task: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("task with ID %d does not exist", t.ID)
	}
	return nil
}

// LoadCommands returns all persisted commands.
func (s *SQLiteStore) LoadCommands() ([]*api.Command, error) {
	var commands []*api.Command
	if err := s.db.Find(&commands).Error; err != nil {
		return nil, fmt.Errorf("failed to load commands: %w", err)
	}
	return commands, nil
}

// LoadTasks returns all persisted tasks.
func (s *SQLiteStore) LoadTasks() ([]*api.Task, error) {
	var tasks []*api.Task
	if err := s.db.Find(&tasks).Error; err != nil {
		return nil, fmt.Errorf("failed to load tasks: %w", err)
	}
	return tasks, nil
}

// FindTasksByCommandID returns all tasks belonging to the given command.
func (s *SQLiteStore) FindTasksByCommandID(commandID uint) ([]*api.Task, error) {
	var tasks []*api.Task
	if err := s.db.Where("command_id = ?", commandID).Find(&tasks).Error; err != nil {
		return nil, fmt.Errorf("failed to find tasks by command ID: %w", err)
	}
	return tasks, nil
}

// NumCommands returns the total number of persisted commands.
func (s *SQLiteStore) NumCommands() uint {
	var count int64
	s.db.Model(&api.Command{}).Count(&count)
	return uint(count)
}

// NumTasks returns the total number of persisted tasks.
func (s *SQLiteStore) NumTasks() uint {
	var count int64
	s.db.Model(&api.Task{}).Count(&count)
	return uint(count)
}
