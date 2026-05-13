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
	UUID      string     `json:"uuid"`
	Status    TaskStatus `json:"status"`
	Artifacts []Artifact `json:"artifacts"`

	// One of Get
	Get *GetTask `json:"get,omitempty"`
}

type GetTask struct {
	// One of Retina, GetIrisTask, GetArkTask
	Retina *GetRetinaTask `json:"retina,omitempty"`
	Iris   *GetIrisTask   `json:"iris,omitempty"`
	Ark    *GetArkTask    `json:"ark,omitempty"`
}

type GetRetinaTask struct {
	Endpoint        string `json:"endpoint"`
	Live            bool   `json:"live"`
	DurationSeconds int64  `json:"duration_seconds" example:"60"`
}

func (t GetRetinaTask) Duration() time.Duration {
	return time.Duration(t.DurationSeconds) * time.Second
}

type GetIrisTask struct {
	Endpoint string `json:"endpoint"`
	// TODO
}

type GetArkTask struct {
	Endpoint string `json:"endpoint"`
	// TODO
}

type Artifact struct {
}

// Server objects.
type ErrorResponse struct {
	Error string `json:"error"`
}

type CreateTaskRequest struct {
	// One of Get
	Get *GetTask `json:"get,omitempty"`
}

type CreateTaskResponse struct {
	TaskUUID string `json:"task_uuid"`
}
