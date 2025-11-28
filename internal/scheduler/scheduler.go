package scheduler

import (
	"context"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

// This is the task scheduler. The usage is that the nodes are added and then the dependencies are frozen to perform the
// compuation. The commands can be added to the queue at any time. Each command would spawn tasks t run one by one. Note
// that the queue is designed to run a single task at a  time.
type Scheduler interface {
	// Adds a command to the queue with the given params and the priority.
	EnqueueCommand(params string, p uint) (*api.Command, error)

	// Removes the command from the queue, stops all its running tasks, marks them as waiting, marks the command as not
	// active.
	DequeueCommand(commandID uint) error

	// Adds the command to the queue again, marks its tasks are idle, marks the command as active.
	RequeueCommand(commandID uint) (*api.Command, error)

	// Starts the execution loop in a separate go routine, cancelling the context would stop execution.
	Start(ctx context.Context) error

	// Gets the current active command ID, if there are none returns an error.
	GetCurrentCommandID() (uint, error)

	// Gets the command with the given ID, if it doesn't exists then returns an error.
	GetCommand(commandID uint) (*api.Command, error)

	// Sets the priority of a command, this would not stop the if a task of this command is running.
	SetPriority(commandID uint, p uint) error

	// ListCommands returns all commands
	ListCommands() ([]api.Command, error)

	// ListAllTasks returns all tasks across all commands
	ListAllTasks() ([]api.Task, error)

	// ListTasksForCommand returns all tasks for a specific command
	ListTasksForCommand(commandID uint) ([]api.Task, error)
}
