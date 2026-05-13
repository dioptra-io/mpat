package api

type TaskStatus string

const (
	TaskStatusQueued    TaskStatus = "queued"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusDone      TaskStatus = "done"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	UUID string `json:"uuid"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type CreateTaskRequest struct {
	Platform   string      `json:"platform" example:"retina"`
	GetCommand *GetCommand `json:"get_command,omitempty"`
}

type CreateTaskResponse struct {
	TaskUUID string `json:"task_uuid"`
	Status   string `json:"status"`
}

type TaskResponse struct {
	TaskUUID        string            `json:"task_uuid"`
	CreationRequest CreateTaskRequest `json:"creation_request"`
	Status          string            `json:"status"`
}

type GetCommand struct {
	Platform string // iris, retina
	// these will be added later.
}
