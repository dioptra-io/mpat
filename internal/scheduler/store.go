package scheduler

import "github.com/dioptra-io/ufuk-research/internal/api"

// Store provides access to persistent Commands and Tasks.
type Store interface {
	// CreateEmptyCommand creates and persists a new empty command.
	CreateEmptyCommand() (*api.Command, error)

	// CreateEmptyTask creates and persists a new empty task.
	CreateEmptyTask() (*api.Task, error)

	// UpdateCommand updates an existing command. Returns an error if it does not exist.
	UpdateCommand(c *api.Command) error

	// UpdateTask updates an existing task. Returns an error if it does not exist.
	UpdateTask(t *api.Task) error

	// LoadCommands returns all persisted commands.
	LoadCommands() ([]*api.Command, error)

	// LoadTasks returns all persisted tasks.
	LoadTasks() ([]*api.Task, error)

	// FindTasksByCommandID returns all tasks belonging to the given command.
	FindTasksByCommandID(commandID uint) ([]*api.Task, error)

	// NumCommands returns the total number of persisted commands.
	NumCommands() uint

	// NumTasks returns the total number of persisted tasks.
	NumTasks() uint
}
