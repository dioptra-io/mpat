package scheduler

import (
	"errors"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Store provides access to persistent Commands and Tasks.
type Store interface {
	// Creates a new command, returns a new command pointer.
	CreateCommand(c *api.Command) error

	// Creates a new task returns a new task pointer.
	CreateTask(t *api.Task) error

	// Loads the command with the commandID also populates the Task fields.
	LoadCommand(commandId uint) (*api.Command, error)

	// Loads the task by the given commandID and nv.
	LoadTask(commandID uint, nv api.NamedVersion) (*api.Task, error)

	// Saves the command.
	UpdateCommand(c *api.Command) error

	// Saves the task.
	UpdateTask(t *api.Task) error

	// Lists all the commandIDs of the commands.
	ListAllCommandIDs() ([]uint, error)

	// Lists all the commandIDs of the commands that are adequate for running.
	ListAvailableCommandIDs() ([]uint, error)
}

type sqliteStore struct {
	db *gorm.DB
}

// Compile time guarantee
var _ Store = (*sqliteStore)(nil)

func NewSQLiteStore(path string) (Store, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	// Auto-migrate your models
	if err := db.AutoMigrate(&api.Command{}, &api.Task{}); err != nil {
		return nil, err
	}

	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) CreateCommand(c *api.Command) error {
	return s.db.Create(c).Error
}

func (s *sqliteStore) CreateTask(t *api.Task) error {
	return s.db.Create(t).Error
}

func (s *sqliteStore) LoadCommand(commandId uint) (*api.Command, error) {
	var cmd api.Command
	err := s.db.
		Preload("Tasks").
		First(&cmd, "id = ?", commandId).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &cmd, err
}

func (s *sqliteStore) LoadTask(commandID uint, nv api.NamedVersion) (*api.Task, error) {
	var task api.Task

	err := s.db.
		First(&task, "command_id = ? AND node_nv = ?", commandID, nv.String()).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &task, err
}

func (s *sqliteStore) UpdateCommand(c *api.Command) error {
	return s.db.Save(c).Error
}

func (s *sqliteStore) UpdateTask(t *api.Task) error {
	return s.db.Save(t).Error
}

func (s *sqliteStore) ListAllCommandIDs() ([]uint, error) {
	var ids []uint
	err := s.db.
		Model(&api.Command{}).
		Pluck("id", &ids).
		Error
	return ids, err
}

func (s *sqliteStore) ListAvailableCommandIDs() ([]uint, error) {
	var ids []uint

	err := s.db.
		Model(&api.Command{}).
		Where("status = ?", api.CommandStatusReady).
		Pluck("id", &ids).
		Error

	return ids, err
}
