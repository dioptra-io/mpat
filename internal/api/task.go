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

	// Task has no associated node at the moment (this was an out to date task)
	TaskStatusOrphaned TaskStatus = "orphaned"
)

// Tasks are spawned for each command and for each node.
type Task struct {
	// Composite primary key: uniquely identifies a task
	CommandID uint         `gorm:"primaryKey;references:ID;constraint:OnDelete:CASCADE" json:"command_id"`
	NodeNV    NamedVersion `gorm:"primaryKey;type:text" json:"node_nv"`

	// Status if the task.
	Status TaskStatus `gorm:"type:varchar(16)" json:"status"`

	// Runtime params of the task.
	Params string `gorm:"type:text" json:"params"`

	// Timestamps.
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

func (t Task) IsFinished() bool {
	return t.Status == TaskStatusFailed || t.Status == TaskStatusCompleted
}
