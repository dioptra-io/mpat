package api

import "time"

// Command states
//
// A Command represents a collection of Tasks. Its status reflects the overall lifecycle of those Tasks and whether the
// Command is currently considered "active" by the scheduler.
//
// A Command is:
//   - "active"     when it is in the active queue.
//   - "inactive"   when it still has unfinished tasks but is not in the active queue.
//   - "failed"     when it is no longer active and at least one task finished with an error.
//   - "completed"  when it is no longer active and all tasks finished successfully.
type CommandStatus string

const (
	// Command is active and currently in the active queue.
	CommandStatusActive CommandStatus = "active"

	// Command has unfinished tasks but is not in the active queue.
	CommandStatusInactive CommandStatus = "inactive"

	// Command is not in the active queue and at least one task failed.
	CommandStatusFailed CommandStatus = "failed"

	// Command is not in the active queue and all tasks succeeded.
	CommandStatusCompleted CommandStatus = "completed"
)

// A command is run with parameters which spawns tasks to run for each node. Then the scheduler runs processes taking
// the dependency of the nodes into account.
type Command struct {
	// ID of the command.
	ID uint `gorm:"primaryKey" json:"id"`

	// Status of the command
	Status CommandStatus `json:"status"`

	// Priority of the command, higher the better.
	Priority uint `json:"priority"`

	// Params for additional information.
	Params string `json:"params"`

	// All tasks created for this command.
	TaskIDs []uint `gorm:"serializer:json" json:"task_ids"`

	// Creation timestamp.
	CreatedAt time.Time `json:"created_at"`

	// Update timestamp.
	FinishedAt time.Time `json:"finished_at"`
}

func (c Command) InActiveQueue() bool {
	return c.Status == CommandStatusActive
}

func (c Command) IsFinished() bool {
	return c.Status == CommandStatusFailed ||
		c.Status == CommandStatusCompleted
}

func (Command) TableName() string {
	return "commands"
}
