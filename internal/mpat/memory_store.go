package mpat

import (
	"context"
	"sync"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/google/uuid"
)

type MPATMemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]api.Task
}

func NewMPATMemoryStore() *MPATMemoryStore {
	return &MPATMemoryStore{
		tasks: make(map[string]api.Task),
	}
}

func (s *MPATMemoryStore) CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.Task, error) {
	if err := ctx.Err(); err != nil {
		return api.Task{}, err
	}

	taskUUID := uuid.NewString()

	task := api.Task{
		UUID:         taskUUID,
		Status:       api.TaskStatus(api.TaskStatusQueued),
		Created:      time.Now(),
		RetinaStream: req.RetinaStream,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[taskUUID] = task

	return task, nil
}

func (s *MPATMemoryStore) GetTask(ctx context.Context, taskUUID string) (api.Task, error) {
	if err := ctx.Err(); err != nil {
		return api.Task{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.tasks[taskUUID]
	if !ok {
		return api.Task{}, ErrTaskNotFound
	}

	return task, nil
}

func (s *MPATMemoryStore) ListTasks(ctx context.Context) ([]api.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]api.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (s *MPATMemoryStore) ListTasksByStatus(ctx context.Context, statuses ...api.TaskStatus) ([]api.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	statusSet := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		statusSet[string(status)] = struct{}{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]api.Task, 0)

	for _, task := range s.tasks {
		if _, ok := statusSet[string(task.Status)]; ok {
			tasks = append(tasks, task)
		}
	}

	return tasks, nil
}

func (s *MPATMemoryStore) UpdateTaskStatus(ctx context.Context, taskUUID string, status api.TaskStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskUUID]
	if !ok {
		return ErrTaskNotFound
	}

	task.Status = api.TaskStatus(status)
	s.tasks[taskUUID] = task

	return nil
}

func (s *MPATMemoryStore) CancelTask(ctx context.Context, taskUUID string) (api.Task, error) {
	if err := ctx.Err(); err != nil {
		return api.Task{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskUUID]
	if !ok {
		return api.Task{}, ErrTaskNotFound
	}

	task.Status = api.TaskStatus(api.TaskStatusCancelled)
	s.tasks[taskUUID] = task

	return task, nil
}
