package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

type InMemoryStore struct {
	mu            sync.RWMutex
	commands      map[uint]*api.Command
	tasks         map[uint]*api.Task
	nextCommandID uint
	nextTaskID    uint
}

func NewInMemoryStore() (*InMemoryStore, error) {
	return &InMemoryStore{
		commands:      make(map[uint]*api.Command),
		tasks:         make(map[uint]*api.Task),
		nextCommandID: 1,
		nextTaskID:    1,
	}, nil
}

// CreateEmptyCommand creates and persists a new empty command.
func (s *InMemoryStore) CreateEmptyCommand() (*api.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := &api.Command{
		ID:        s.nextCommandID,
		TaskIDs:   []uint{},
		CreatedAt: time.Now(),
	}
	s.commands[cmd.ID] = cmd
	s.nextCommandID++

	return cmd, nil
}

// CreateEmptyTask creates and persists a new empty task.
func (s *InMemoryStore) CreateEmptyTask() (*api.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := &api.Task{
		ID:        s.nextTaskID,
		CreatedAt: time.Now(),
	}
	s.tasks[task.ID] = task
	s.nextTaskID++

	return task, nil
}

// UpdateCommand updates an existing command. Returns an error if it does not exist.
func (s *InMemoryStore) UpdateCommand(c *api.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.commands[c.ID]; !exists {
		return fmt.Errorf("command with ID %d does not exist", c.ID)
	}

	// Create a copy to avoid external modifications
	cmdCopy := *c
	s.commands[c.ID] = &cmdCopy

	return nil
}

// UpdateTask updates an existing task. Returns an error if it does not exist.
func (s *InMemoryStore) UpdateTask(t *api.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[t.ID]; !exists {
		return fmt.Errorf("task with ID %d does not exist", t.ID)
	}

	// Create a copy to avoid external modifications
	taskCopy := *t
	s.tasks[t.ID] = &taskCopy

	return nil
}

// LoadCommands returns all persisted commands.
func (s *InMemoryStore) LoadCommands() ([]*api.Command, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	commands := make([]*api.Command, 0, len(s.commands))
	for _, cmd := range s.commands {
		// Create a copy to avoid external modifications
		cmdCopy := *cmd
		commands = append(commands, &cmdCopy)
	}

	return commands, nil
}

// LoadTasks returns all persisted tasks.
func (s *InMemoryStore) LoadTasks() ([]*api.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*api.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		// Create a copy to avoid external modifications
		taskCopy := *task
		tasks = append(tasks, &taskCopy)
	}

	return tasks, nil
}

// FindTasksByCommandID returns all tasks belonging to the given command.
func (s *InMemoryStore) FindTasksByCommandID(commandID uint) ([]*api.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*api.Task, 0)
	for _, task := range s.tasks {
		if task.CommandID == commandID {
			// Create a copy to avoid external modifications
			taskCopy := *task
			tasks = append(tasks, &taskCopy)
		}
	}

	return tasks, nil
}

// NumCommands returns the total number of persisted commands.
func (s *InMemoryStore) NumCommands() uint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return uint(len(s.commands))
}

// NumTasks returns the total number of persisted tasks.
func (s *InMemoryStore) NumTasks() uint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return uint(len(s.tasks))
}
