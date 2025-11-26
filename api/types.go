package api

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Task states
type Status string

const (
	// Task is created and ready to be run if dependencies are resolved.
	StatusIdle Status = "idle"

	// Task is in the state of running it's task.
	StatusRunning Status = "running"

	// Task is stopped. Can be resumed later.
	StatusWaiting Status = "waiting"

	// Task had encountered an error.
	StatusFailed Status = "failed"

	// Task completed successtully.
	StatusDone Status = "done"
)

// Represents a node and a verison. It is written as <node-name>/v<node-version>. For example:
// "ingestion_node/v2".
type NamedVersion struct {
	Name    string `json:"name"`
	Version uint   `json:"version"`
}

func NewNamedVersion(name string, version uint) NamedVersion {
	return NamedVersion{
		Name:    name,
		Version: version,
	}
}

// Value implements the driver.Valuer interface for GORM
func (nv NamedVersion) Value() (driver.Value, error) {
	return json.Marshal(nv)
}

// Scan implements the sql.Scanner interface for GORM
func (nv *NamedVersion) Scan(value any) error {
	if value == nil {
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("failed to scan NamedVersion: unsupported type %T", value)
	}

	return json.Unmarshal(bytes, nv)
}

func (nv NamedVersion) String() string {
	return fmt.Sprintf("%s/v%d", nv.Name, nv.Version)
}

// A command is run with parameters which spawns processes to run for each node. Then the server
// runs processes taking the dependency of the nodes into account.
type Command struct {
	// ID of the process.
	ID uint `gorm:"primaryKey" json:"id"`

	// Priority of the comamnd, higher the better.
	Priority uint `json:"priority"`

	// Params for additional information.
	Params string `json:"payload"`

	// All processes connected to this.
	// NOTE: Not stored in DB — populated manually for JSON responses.
	TaskIDs []uint `gorm:"-" json:"task_ids"`

	// Tasks
	Tasks []Task `gorm:"foreignKey:CommandID" json:"tasks"`

	// Creation timestamp.
	CreatedAt time.Time `json:"created_at"`

	// Active means if a command is in the queue for execution or not.
	Active bool `json:"active"`
}

func (Command) TableName() string {
	return "commands"
}

// Tasks are spawned for each command and for each node.
type Task struct {
	// ID of the process.
	ID uint `gorm:"primaryKey" json:"id"`

	// CommandID; all processes belong to a command.
	CommandID uint `json:"command_id"`

	// Name of the node that this process is assigned.
	NodeNamedVersion NamedVersion `json:"node_named_version"`

	// Creation time.
	CreatedAt time.Time `json:"created_at"`

	// Status of the process.
	Status Status `json:"status"`

	// Params for additional information.
	Params string `json:"params"`

	// Orphan marks tasks whose node no longer exists
	Orphan bool `json:"orphan"`
}

func (Task) TableName() string {
	return "tasks"
}
