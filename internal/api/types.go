package api

import (
	"encoding/json"
	"fmt"
	"time"

	retina "github.com/dioptra-io/retina-commons/api/v1"
)

type TaskStatus string

const (
	TaskStatusQueued TaskStatus = "queued"

	TaskStatusRunning TaskStatus = "running"

	TaskStatusDone      TaskStatus = "done"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type TaskType string

const (
	TaskTypeUnknown      TaskType = "unkwnown"
	TaskTypeRetinaStream TaskType = "retina_stream"
)

type Task struct {
	UUID    string          `json:"uuid"`
	Type    TaskType        `json:"type"`
	Status  TaskStatus      `json:"status"`
	Created time.Time       `json:"created"`
	Payload json.RawMessage `json:"payload"`
}

type RetinaStreamTaskRequest struct {
	Endpoint        string `json:"endpoint"`
	DurationSeconds int64  `json:"duration_seconds" example:"60"`
	OutputFile      string `json:"output_file"`
}

// Server objects.
type ErrorResponse struct {
	Error string `json:"error"`
}

type CreateTaskRequest struct {
	// One of
	RetinaStream *RetinaStreamTaskRequest `json:"retina_stream,omitempty"`
}

type CreateTaskResponse struct {
	TaskUUID string `json:"task_uuid"`
}

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered delivery to HTTP clients.
type SequencedFIE struct {
	retina.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}
