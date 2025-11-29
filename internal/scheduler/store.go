package scheduler

import "github.com/dioptra-io/ufuk-research/internal/api"

// Store provides access to persistent Commands and Tasks.
type Store interface {
	// Creates a new command with the given payload, it also creates the Tasks.
	CreateCommand(payload string) (*api.Command, error)

	// Loads the command with the commandID also populates the Task fields.
	LoadCommand(commandId uint) (*api.Command, error)

	// Saves the command and its tasks.
	SaveCommand(c *api.Command) error

	// Gets all the commandIDs of the commands.
	GetAllCommandIDs() ([]uint, error)
}
