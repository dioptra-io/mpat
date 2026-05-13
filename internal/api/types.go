package api

import (
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
	UUID    string     `json:"uuid"`
	Status  TaskStatus `json:"status"`
	Created time.Time  `json:"created"`

	// One of
	RetinaStream *RetinaStreamTaskRequest `json:"retina_stream,omitempty"`
}

func (t *Task) Type() TaskType {
	if t.RetinaStream != nil {
		return TaskTypeRetinaStream
	}
	return TaskTypeUnknown
}

func (t *Task) Args() map[string]string {
	switch t.Type() {
	case TaskTypeRetinaStream:
		if t.RetinaStream == nil {
			return map[string]string{}
		}

		args := map[string]string{
			"duration": fmt.Sprintf("%ds", t.RetinaStream.DurationSeconds),
			"output":   t.RetinaStream.OutputFile,
		}

		if t.RetinaStream.Endpoint != DefaultRetinaStreamEndpoint {
			args["endpoint"] = t.RetinaStream.Endpoint
		}

		return args

	default:
		return map[string]string{}
	}
}

type RetinaStreamTaskRequest struct {
	Endpoint        string `json:"endpoint"`
	DurationSeconds int64  `json:"duration_seconds" example:"60"`
	OutputFile      string `json:"output_file"`
}

func (t RetinaStreamTaskRequest) Duration() time.Duration {
	return time.Duration(t.DurationSeconds) * time.Second
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
