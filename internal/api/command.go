package api

import (
	"time"
)

// CommandStatus describes the scheduler’s view of a Command’s lifecycle.
//
// A Command can be:
//   - "ready":     queued and waiting to be started.
//   - "running":   currently executing.
//   - "sleeping":  intentionally paused by the user; the scheduler leaves it alone.
//   - "failed":    execution finished and at least one task ended in error.
//   - "completed": execution finished and all tasks succeeded.
type CommandStatus string

const (
	// Command is ready for execution.
	CommandStatusReady CommandStatus = "ready"

	// Command is running.
	CommandStatusRunning CommandStatus = "running"

	// Command is marked as sleeping by the user, we don't disturb.
	CommandStatusSleeping CommandStatus = "sleeping"

	// Command is not in the active queue and at least one task failed.
	CommandStatusFailed CommandStatus = "failed"

	// Command is not in the active queue and all tasks succeeded.
	CommandStatusCompleted CommandStatus = "completed"
)

// A command is run with parameters which spawns tasks to run for each node. Then the scheduler runs processes taking
// the dependency of the nodes into account.
type Command struct {
	ID uint `gorm:"primaryKey" json:"id"`

	Status   CommandStatus `gorm:"type:varchar(16)" json:"status"`
	Priority uint          `json:"priority"`
	Payload  string        `gorm:"type:text" json:"payload"`

	// Timestamps.
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	FinishedAt *time.Time `json:"finished_at"`

	// Tasks associated with this command.
	Tasks []Task `gorm:"foreignKey:CommandID;constraint:OnDelete:CASCADE" json:"tasks,omitempty"`
}

func (c Command) InActiveQueue() bool {
	return c.Status == CommandStatusReady || c.Status == CommandStatusRunning
}

func (c Command) IsFinished() bool {
	return c.Status == CommandStatusFailed || c.Status == CommandStatusCompleted
}
