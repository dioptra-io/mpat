package api

import (
	"time"
)

// Task states
//
// A Task starts in the "ready" state when it is created. If the server cannot find the Node associated with this task,
// it becomes an "orphan".
//
// When it's time to run, the task moves to "running". From there:
//
//   - If execution fails, the task is marked as "failed".
//   - If execution succeeds, the task is marked as "completed".
//   - If the command associated with the task becomes inactive, the task is put to "sleeping". It will return to
//     "ready" once the command becomes active again.
type TaskStatus string

const (
	// Command is active; task is ready to run.
	TaskStatusReady TaskStatus = "ready"

	// Command is active; task is currently running.
	TaskStatusRunning TaskStatus = "running"

	// Command is active; task finished with an error.
	TaskStatusFailed TaskStatus = "failed"

	// Task finished successfully (command may be active or inactive).
	TaskStatusCompleted TaskStatus = "completed"
)

// Tasks are spawned for each command and for each node.
type Task struct {
	// Status of the process.
	Status TaskStatus `json:"status"`

	// Params for additional information.
	Params string `json:"params"`

	// CommandID; all processes belong to a command.
	CommandID uint `json:"command_id"`

	// Name of the node that this process is assigned.
	NodeNV NamedVersion `json:"node_nv"`

	// Timestamp of creation.
	CreatedAt time.Time `json:"created_at"`

	// Timestamp of task marked as failed or done status.
	FinishedAt time.Time `json:"finished_at"`
}

func (t Task) IsFinished() bool {
	return t.Status == TaskStatusFailed ||
		t.Status == TaskStatusCompleted
}

func (t Task) CanRun() bool {
	return t.Status == TaskStatusReady ||
		t.Status == TaskStatusRunning
}

func (Task) TableName() string {
	return "tasks"
}
