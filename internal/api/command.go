package api

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

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

	// Payload for additional information.
	Payload string `json:"payload"`

	// All tasks created for this command.
	Tasks TaskMap `gorm:"type:text" json:"-"` // Don't use default JSON marshaling

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

// Finish sets the command and all tasks state to "failed" or "completed"
func (c *Command) MarkAsFinished(success bool) {
	c.FinishedAt = time.Now()

	if success {
		c.Status = CommandStatusCompleted
		// Mark all non-terminal tasks as completed
		for nv, task := range c.Tasks {
			if task.Status != TaskStatusCompleted && task.Status != TaskStatusFailed {
				task.Status = TaskStatusCompleted
				task.FinishedAt = time.Now()
				c.Tasks[nv] = task
			}
		}
	} else {
		c.Status = CommandStatusFailed
		// Mark all non-terminal tasks as failed
		for nv, task := range c.Tasks {
			if task.Status != TaskStatusCompleted && task.Status != TaskStatusFailed {
				task.Status = TaskStatusFailed
				task.FinishedAt = time.Now()
				c.Tasks[nv] = task
			}
		}
	}
}

func (Command) TableName() string {
	return "commands"
}

// TaskMap is a custom type for map[NamedVersion]Task that implements sql.Scanner and driver.Valuer
type TaskMap map[NamedVersion]*Task

// Value implements the driver.Valuer interface for GORM
func (tm TaskMap) Value() (driver.Value, error) {
	if tm == nil {
		return "{}", nil
	}
	return json.Marshal(tm)
}

// MarshalJSON implements custom JSON marshaling for Command
func (c Command) MarshalJSON() ([]byte, error) {
	// Convert Tasks map to slice for JSON
	tasks := make([]*Task, 0, len(c.Tasks))
	for _, task := range c.Tasks {
		tasks = append(tasks, task)
	}

	type Alias Command
	return json.Marshal(&struct {
		Tasks []*Task `json:"tasks"`
		*Alias
	}{
		Tasks: tasks,
		Alias: (*Alias)(&c),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for Command
func (c *Command) UnmarshalJSON(data []byte) error {
	type Alias Command
	aux := &struct {
		Tasks []*Task `json:"tasks"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	// Convert slice back to map
	c.Tasks = make(TaskMap)
	for _, task := range aux.Tasks {
		if task != nil {
			c.Tasks[task.NodeNV] = task
		}
	}

	return nil
}
