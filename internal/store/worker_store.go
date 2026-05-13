package store

import (
	"context"
	"errors"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

var ErrTaskNotFound = errors.New("task not found")

type WorkerStore interface {
	CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.Task, error)
	GetTask(ctx context.Context, taskUUID string) (api.TaskResponse, error)
	ListTasks(ctx context.Context) ([]api.TaskResponse, error)
	ListTasksByStatus(ctx context.Context, statuses ...api.TaskStatus) ([]api.TaskResponse, error)
	UpdateTaskStatus(ctx context.Context, taskUUID string, status api.TaskStatus) error
	CancelTask(ctx context.Context, taskUUID string) (api.TaskResponse, error)
}
