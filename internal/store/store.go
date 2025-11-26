package store

import (
	"github.com/dioptra-io/ufuk-research/api"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Your GORM-backed store
type SQLiteStore struct {
	db *gorm.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto-create schema
	err = db.AutoMigrate(&api.Command{}, &api.Task{})
	if err != nil {
		return nil, err
	}

	return &SQLiteStore{db: db}, nil
}

// GetCommands returns all commands in the database.
func (s *SQLiteStore) GetCommands() ([]api.Command, error) {
	var cmds []api.Command
	if err := s.db.Preload("Processes").Find(&cmds).Error; err != nil {
		return nil, err
	}

	// Populate ProcessIDs manually
	for i := range cmds {
		cmds[i].TaskIDs = make([]uint, len(cmds[i].Tasks))
		for j, p := range cmds[i].Tasks {
			cmds[i].TaskIDs[j] = p.ID
		}
	}

	return cmds, nil
}

// GetProcesses returns all processes in the database.
func (s *SQLiteStore) GetProcesses() ([]api.Task, error) {
	var procs []api.Task
	if err := s.db.Find(&procs).Error; err != nil {
		return nil, err
	}
	return procs, nil
}

// SaveCommand creates or updates a command in the database.
func (s *SQLiteStore) SaveCommand(cmd *api.Command) error {
	return s.db.Save(cmd).Error
}

// SaveProcess creates or updates a process in the database.
func (s *SQLiteStore) SaveProcess(p *api.Task) error {
	return s.db.Save(p).Error
}
