package mpat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"encoding/json"

	"github.com/dioptra-io/ufuk-research/internal/api"

	"golang.org/x/sync/errgroup"

	_ "github.com/dioptra-io/ufuk-research/docs"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

type MPATServerConfig struct {
	Addr       string
	QueueSize  int
	NumWorkers int
}

func (cfg *MPATServerConfig) validate() error {
	if cfg.Addr == "" {
		cfg.Addr = "localhost:9293"
	}

	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}

	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 1
	}

	return nil
}

type MPATServer struct {
	queue      chan string
	server     *http.Server
	store      MPATStore
	logger     *slog.Logger
	numWorkers int

	taskCancelMu sync.Mutex
	taskCancelCh map[string]chan struct{}
}

func NewServer(cfg MPATServerConfig, workerStore MPATStore, logger *slog.Logger) (*MPATServer, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("worker config cannot be validated: %w", err)
	}

	if workerStore == nil {
		return nil, fmt.Errorf("worker store cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	w := &MPATServer{
		queue:        make(chan string, cfg.QueueSize),
		store:        workerStore,
		logger:       logger,
		numWorkers:   cfg.NumWorkers,
		taskCancelCh: make(map[string]chan struct{}),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", w.handleGetHealthz)

	// tasks
	mux.HandleFunc("GET /tasks", w.handleGetTasks)
	mux.HandleFunc("GET /tasks/canceled", w.handleGetCanceledTasks)
	mux.HandleFunc("GET /tasks/failed", w.handleGetFailedTasks)
	mux.HandleFunc("GET /tasks/queued", w.handleGetQueuedTasks)
	mux.HandleFunc("GET /tasks/done", w.handleGetDoneTasks)
	mux.HandleFunc("GET /tasks/terminated", w.handleGetTerminatedTasks)
	mux.HandleFunc("GET /tasks/running", w.handleGetRunningTasks)

	mux.HandleFunc("GET /tasks/{task_uuid}", w.handleGetTask)

	mux.HandleFunc("POST /tasks/{task_uuid}/cancel", w.handlePostCancelTask)

	mux.HandleFunc("POST /tasks", w.handlePostTasks)

	// swagger
	mux.Handle("/swagger/", httpSwagger.WrapHandler)

	w.server = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return w, nil
}

func (w *MPATServer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	if err := w.recoverUnterminatedTasks(ctx); err != nil {
		return err
	}

	for i := 0; i < w.numWorkers; i++ {
		i := i
		g.Go(func() error {
			w.runWorker(ctx, i)
			return nil
		})
	}

	g.Go(func() error {
		w.logger.Info("starting worker http server", "addr", w.server.Addr)

		err := w.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	g.Go(func() error {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		w.logger.Info("shutting down worker http server")

		if err := w.server.Shutdown(shutdownCtx); err != nil {
			return err
		}

		return ctx.Err()
	})

	return g.Wait()
}

func (w *MPATServer) recoverUnterminatedTasks(ctx context.Context) error {
	w.logger.Info("recovering unterminated tasks")

	tasks, err := w.store.ListTasksByStatus(
		ctx,
		api.TaskStatusQueued,
		api.TaskStatusRunning,
	)
	if err != nil {
		return fmt.Errorf("failed to list unterminated tasks: %w", err)
	}

	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		switch api.TaskStatus(task.Status) {
		case api.TaskStatusRunning:
			w.logger.Warn(
				"marking interrupted running task as failed",
				"task_uuid", task.UUID,
			)

			if err := w.store.UpdateTaskStatus(ctx, task.UUID, api.TaskStatusFailed); err != nil {
				return fmt.Errorf("failed to mark running task as failed (%s): %w", task.UUID, err)
			}

		case api.TaskStatusQueued:
			w.logger.Info(
				"re-queueing queued task",
				"task_uuid", task.UUID,
			)

			select {
			case w.queue <- task.UUID:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	w.logger.Info(
		"unterminated task recovery complete",
		"recovered_tasks", len(tasks),
	)

	return nil
}

func (w *MPATServer) runWorker(ctx context.Context, id int) {
	w.logger.Info("worker started", "worker_id", id)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker stopped", "worker_id", id)
			return

		case taskUUID := <-w.queue:
			taskCancelCh := make(chan struct{})

			w.taskCancelMu.Lock()
			w.taskCancelCh[taskUUID] = taskCancelCh
			w.taskCancelMu.Unlock()

			w.processTask(ctx, id, taskUUID, taskCancelCh)

			w.taskCancelMu.Lock()
			delete(w.taskCancelCh, taskUUID)
			w.taskCancelMu.Unlock()
		}
	}
}

func (w *MPATServer) processTask(ctx context.Context, workerID int, taskUUID string, taskCancelCh <-chan struct{}) {
	w.logger.Info(
		"processing task",
		"worker_id", workerID,
		"task_uuid", taskUUID,
	)

	if err := w.store.UpdateTaskStatus(ctx, taskUUID, api.TaskStatusRunning); err != nil {
		w.logger.Error("failed to mark task as running", "task_uuid", taskUUID, "error", err)
		return
	}

	select {
	case <-ctx.Done():
		if err := w.store.UpdateTaskStatus(context.Background(), taskUUID, api.TaskStatusCancelled); err != nil {
			w.logger.Error("failed to mark task as cancelled", "task_uuid", taskUUID, "error", err)
		}
		return

	case <-taskCancelCh:
		if err := w.store.UpdateTaskStatus(context.Background(), taskUUID, api.TaskStatusCancelled); err != nil {
			w.logger.Error("failed to mark task as cancelled", "task_uuid", taskUUID, "error", err)
		}
		return

	case <-time.NewTimer(time.Second * 10).C:
		task, err := w.store.GetTask(ctx, taskUUID)
		if err != nil {
			w.logger.Error("failed to mark task as cancelled", "task_uuid", taskUUID, "error", err)
		}
		slog.Info("processed task", "task_uuid", taskUUID)

		// DO a selection here for how to process the task.
		if task.RetinaStream != nil {
			fmt.Printf("task.RetinaStream.OutputFile: %v\n", task.RetinaStream.OutputFile)
		} else {
			slog.Info("unknown task type", "task_uuid", taskUUID)
			if err := w.store.UpdateTaskStatus(ctx, taskUUID, api.TaskStatusFailed); err != nil {
				w.logger.Error("failed to mark task as done", "task_uuid", taskUUID, "error", err)
				return
			}
		}
	}

	if err := w.store.UpdateTaskStatus(ctx, taskUUID, api.TaskStatusDone); err != nil {
		w.logger.Error("failed to mark task as done", "task_uuid", taskUUID, "error", err)
		return
	}
}

// handleGetHealthz godoc
//
// @Summary      Health check
// @Description  Returns ok if the MPAT worker server is alive.
// @Tags         health
// @Produce      plain
// @Success      200  {string}  string  "ok"
// @Router       /healthz [get]
func (w *MPATServer) handleGetHealthz(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write([]byte("ok\n"))
}

// handlePostTasks godoc
//
// @Summary      Create task
// @Description  Creates a new task, stores it, and enqueues it for execution.
// @Tags         tasks
// @Accept       json
// @Produce      json
// @Param        request  body      api.CreateTaskRequest  true  "Task creation request"
// @Success      202      {object}  api.CreateTaskResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Failure      500      {object}  api.ErrorResponse
// @Router       /tasks [post]
func (w *MPATServer) handlePostTasks(rw http.ResponseWriter, r *http.Request) {
	var req api.CreateTaskRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(rw, http.StatusBadRequest, "invalid json body")
		return
	}

	task, err := w.store.CreateTask(r.Context(), req)
	if err != nil {
		w.logger.Error("failed to create task", "error", err)
		writeError(rw, http.StatusInternalServerError, "failed to create task")
		return
	}

	select {
	case w.queue <- task.UUID:
		writeJSON(rw, http.StatusAccepted, api.CreateTaskResponse{
			TaskUUID: task.UUID,
		})
	default:
		if err := w.store.UpdateTaskStatus(r.Context(), task.UUID, api.TaskStatusFailed); err != nil {
			w.logger.Error("failed to mark task as failed after queue overflow", "task_uuid", task.UUID, "error", err)
		}

		writeError(rw, http.StatusServiceUnavailable, "task queue is full")
	}
}

// handleGetTasks godoc
//
// @Summary      List tasks
// @Description  Returns all known tasks.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks [get]
func (w *MPATServer) handleGetTasks(rw http.ResponseWriter, r *http.Request) {
	tasks, err := w.store.ListTasks(r.Context())
	if err != nil {
		w.logger.Error("failed to list tasks", "error", err)
		writeError(rw, http.StatusInternalServerError, "failed to list tasks")
		return
	}

	writeJSON(rw, http.StatusOK, tasks)
}

// handleGetTask godoc
//
// @Summary      Get task
// @Description  Returns a task by UUID.
// @Tags         tasks
// @Produce      json
// @Param        task_uuid  path      string  true  "Task UUID"
// @Success      200        {object}  api.Task
// @Failure      400        {object}  api.ErrorResponse
// @Failure      404        {object}  api.ErrorResponse
// @Failure      500        {object}  api.ErrorResponse
// @Router       /tasks/{task_uuid} [get]
func (w *MPATServer) handleGetTask(rw http.ResponseWriter, r *http.Request) {
	taskUUID := r.PathValue("task_uuid")
	if taskUUID == "" {
		writeError(rw, http.StatusBadRequest, "task uuid is required")
		return
	}

	task, err := w.store.GetTask(r.Context(), taskUUID)
	if errors.Is(err, ErrTaskNotFound) {
		writeError(rw, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		w.logger.Error("failed to get task", "task_uuid", taskUUID, "error", err)
		writeError(rw, http.StatusInternalServerError, "failed to get task")
		return
	}

	writeJSON(rw, http.StatusOK, task)
}

// handlePostCancelTask godoc
//
// @Summary      Cancel task
// @Description  Cancels a running task by UUID.
// @Tags         tasks
// @Produce      json
// @Param        task_uuid  path      string  true  "Task UUID"
// @Success      200        {object}  api.CreateTaskResponse
// @Failure      400        {object}  api.ErrorResponse
// @Failure      404        {object}  api.ErrorResponse
// @Failure      500        {object}  api.ErrorResponse
// @Router       /tasks/{task_uuid}/cancel [post]
func (w *MPATServer) handlePostCancelTask(rw http.ResponseWriter, r *http.Request) {
	taskUUID := r.PathValue("task_uuid")
	if taskUUID == "" {
		writeError(rw, http.StatusBadRequest, "task uuid is required")
		return
	}

	w.taskCancelMu.Lock()
	taskCancelCh, ok := w.taskCancelCh[taskUUID]
	if ok {
		delete(w.taskCancelCh, taskUUID)
	}
	w.taskCancelMu.Unlock()

	if !ok {
		task, err := w.store.CancelTask(r.Context(), taskUUID)
		if errors.Is(err, ErrTaskNotFound) {
			writeError(rw, http.StatusNotFound, "task not found")
			return
		}
		if err != nil {
			w.logger.Error("failed to cancel task", "task_uuid", taskUUID, "error", err)
			writeError(rw, http.StatusInternalServerError, "failed to cancel task")
			return
		}

		writeJSON(rw, http.StatusOK, api.CreateTaskResponse{
			TaskUUID: task.UUID,
		})
		return
	}

	select {
	case taskCancelCh <- struct{}{}:
	default:
	}

	writeJSON(rw, http.StatusOK, api.CreateTaskResponse{
		TaskUUID: taskUUID,
	})
}

// handleGetQueuedTasks godoc
//
// @Summary      List queued tasks
// @Description  Returns tasks that are currently queued.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/queued [get]
func (w *MPATServer) handleGetQueuedTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(rw, r, api.TaskStatusQueued)
}

// handleGetRunningTasks godoc
//
// @Summary      List running tasks
// @Description  Returns tasks that are currently running.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/running [get]
func (w *MPATServer) handleGetRunningTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(rw, r, api.TaskStatusRunning)
}

// handleGetDoneTasks godoc
//
// @Summary      List completed tasks
// @Description  Returns tasks that completed successfully.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/done [get]
func (w *MPATServer) handleGetDoneTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(rw, r, api.TaskStatusDone)
}

// handleGetFailedTasks godoc
//
// @Summary      List failed tasks
// @Description  Returns tasks that failed during execution.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/failed [get]
func (w *MPATServer) handleGetFailedTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(rw, r, api.TaskStatusFailed)
}

// handleGetCanceledTasks godoc
//
// @Summary      List canceled tasks
// @Description  Returns tasks that were canceled.
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/canceled [get]
func (w *MPATServer) handleGetCanceledTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(rw, r, api.TaskStatusCancelled)
}

// handleGetTerminatedTasks godoc
//
// @Summary      List terminated tasks
// @Description  Returns tasks that reached a terminal state (done, failed, or canceled).
// @Tags         tasks
// @Produce      json
// @Success      200  {array}   api.Task
// @Failure      500  {object}  api.ErrorResponse
// @Router       /tasks/terminated [get]
func (w *MPATServer) handleGetTerminatedTasks(rw http.ResponseWriter, r *http.Request) {
	w.handleTasksByStatus(
		rw,
		r,
		api.TaskStatusFailed,
		api.TaskStatusDone,
		api.TaskStatusCancelled,
	)
}
func (w *MPATServer) handleTasksByStatus(rw http.ResponseWriter, r *http.Request, status ...api.TaskStatus) {
	ctx := r.Context()

	tasks, err := w.store.ListTasksByStatus(ctx, status...)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(rw).Encode(tasks); err != nil {
		w.logger.Error("failed to encode tasks response", "err", err)
	}
}

func writeJSON(rw http.ResponseWriter, status int, value any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)

	if err := json.NewEncoder(rw).Encode(value); err != nil {
		slog.Error("failed to encode json response", "error", err)
	}
}

func writeError(rw http.ResponseWriter, status int, message string) {
	writeJSON(rw, status, api.ErrorResponse{
		Error: message,
	})
}
