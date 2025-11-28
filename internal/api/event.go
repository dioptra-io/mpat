package api

type Event string

// Events for the Commands.
const (
	// Command was created.
	CommandCreated Event = "command_created"

	// Command was moved to the active queue.
	CommandActivated Event = "command_activated"

	// Command was removed from the active queue (but still has unfinished tasks).
	CommandDeactivated Event = "command_deactivated"

	// Command finished with at least one failed task.
	CommandFailed Event = "command_failed"

	// Command finished successfully (all tasks completed).
	CommandCompleted Event = "command_completed"
)

// Events for the Tasks.
const (
	// Task was created.
	TaskCreated Event = "task_created"

	// Task became orphaned because its node no longer exists.
	TaskOrphaned Event = "task_orphaned"

	// Task is blocked because its command is inactive.
	TaskBlocked Event = "task_blocked"

	// Task execution was interrupted; runing --> sleeping.
	TaskInterrupted Event = "task_interrupted"

	// Task was woken up and returned to the ready state.
	TaskWakeup Event = "task_wakeup"

	// Task finished with an error.
	TaskFailed Event = "task_failed"

	// Task finished successfully.
	TaskCompleted Event = "task_completed"
)

func (e Event) String() string {
	return string(e)
}
