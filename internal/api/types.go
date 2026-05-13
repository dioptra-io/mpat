package api

import "time"

type TaskStatus string

const (
	TaskStatusQueued TaskStatus = "queued"

	TaskStatusRunning TaskStatus = "running"

	TaskStatusDone      TaskStatus = "done"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	UUID   string     `json:"uuid"`
	Status TaskStatus `json:"status"`

	// One of
	RetinaStream *RetinaStreamTaskRequest `json:"retina_stream,omitempty"`
}

func (t *Task) Type() string {
	if t.RetinaStream != nil {
		return "retina_stream"
	}
	return "unkwnown"
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
