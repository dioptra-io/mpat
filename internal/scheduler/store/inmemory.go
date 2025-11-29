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
	nextCommandID uint
}

func NewInMemoryStore() (*InMemoryStore, error) {
	return &InMemoryStore{
		commands:      make(map[uint]*api.Command),
		nextCommandID: 1,
	}, nil
}

// CreateCommand creates a new command with the given payload, it also creates the Tasks.
func (s *InMemoryStore) CreateCommand(payload string) (*api.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := &api.Command{
		ID:        s.nextCommandID,
		Payload:   payload,
		Tasks:     make(map[api.NamedVersion]*api.Task),
		CreatedAt: time.Now(),
	}
	s.commands[cmd.ID] = cmd
	s.nextCommandID++

	return cmd, nil
}

// LoadCommand loads the command with the commandID also populates the Task fields.
func (s *InMemoryStore) LoadCommand(commandID uint) (*api.Command, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cmd, exists := s.commands[commandID]
	if !exists {
		return nil, fmt.Errorf("command with ID %d does not exist", commandID)
	}

	// Create a deep copy
	cmdCopy := *cmd
	cmdCopy.Tasks = make(map[api.NamedVersion]*api.Task, len(cmd.Tasks))
	for k, v := range cmd.Tasks {
		cmdCopy.Tasks[k] = v
	}

	return &cmdCopy, nil
}

// SaveCommand saves the command and its tasks.
func (s *InMemoryStore) SaveCommand(c *api.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.commands[c.ID]; !exists {
		return fmt.Errorf("command with ID %d does not exist", c.ID)
	}

	// Create a deep copy
	cmdCopy := *c
	cmdCopy.Tasks = make(map[api.NamedVersion]*api.Task, len(c.Tasks))
	for k, v := range c.Tasks {
		cmdCopy.Tasks[k] = v
	}

	s.commands[c.ID] = &cmdCopy
	return nil
}

// GetAllCommandIDs gets all the commandIDs of the commands.
func (s *InMemoryStore) GetAllCommandIDs() ([]uint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]uint, 0, len(s.commands))
	for id := range s.commands {
		ids = append(ids, id)
	}

	return ids, nil
}
