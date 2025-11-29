package scheduler

import "errors"

var (
	// Command does not exist.
	ErrCommandNotFound = errors.New("command not found")

	// Task does not exist.
	ErrTaskNotFound = errors.New("task not found")

	// The command is finished and can no longer transition to another active state.
	ErrCommandFinished = errors.New("command is finished")

	// The command is sleeping, so dequeueing it again is meaningless.
	ErrCommandAlreadySleeping = errors.New("command already sleeping")

	// The command cannot be requeued because it is not sleeping.
	ErrCommandNotSleeping = errors.New("command is not sleeping")

	// No command is currently being executed.
	ErrNoActiveCommand = errors.New("no active command")

	// No command is ready to be scheduled.
	ErrNoReadyCommand = errors.New("no ready command available")

	// No task is available to run under this command.
	ErrNoAvailableTask = errors.New("no task available")

	// The task under the command was failed.
	ErrTaskFailed = errors.New("task was failed")

	// The task was running before.
	ErrTaskAlreadyRunning = errors.New("task is already running")
)
