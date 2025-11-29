package scheduler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// SchedulerAPI wraps the scheduler with HTTP API capabilities
type SchedulerAPI interface {
	// SetupHandlers configures HTTP routes on the provided router
	SetupHandlers(router *gin.Engine)

	// Start starts the HTTP server in the same goroutine. Returns error channel that signals when HTTP server exits
	Run(ctx context.Context) error
}

type schedulerAPI struct {
	scheduler Scheduler
	logger    logrus.FieldLogger
	server    *http.Server
	router    *gin.Engine
}

var _ SchedulerAPI = (*schedulerAPI)(nil)

// NewSchedulerAPI creates a new SchedulerAPI wrapper around a scheduler
func NewSchedulerAPI(sched Scheduler, logger logrus.FieldLogger) SchedulerAPI {
	return &schedulerAPI{
		scheduler: sched,
		logger:    logger,
	}
}

// SetupHandlers configures HTTP routes on the provided router
func (s *schedulerAPI) SetupHandlers(router *gin.Engine) {
	s.router = router

	// Health check
	router.GET("/health", s.handleHealth)

	// API v1
	v1 := router.Group("/api/v1")
	{
		// Commands
		commands := v1.Group("/commands")
		{
			commands.GET("", s.handleListCommands)
			commands.POST("", s.handleEnqueueCommand)
			commands.GET("/current", s.handleGetCurrentCommand)
			commands.GET("/:id", s.handleGetCommand)
			commands.DELETE("/:id", s.handleDequeueCommand)
			commands.POST("/:id/requeue", s.handleRequeueCommand)
			commands.PUT("/:id/priority", s.handleSetPriority)
			commands.GET("/:id/tasks", s.handleListCommandTasks)
		}

		// Tasks
		tasks := v1.Group("/tasks")
		{
			tasks.GET("", s.handleListAllTasks)
		}

		// Status
		v1.GET("/status", s.handleGetStatus)
	}

	// Create HTTP server
	s.server = &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

}

// Start starts the HTTP server in the same goroutine. Returns error channel that signals when HTTP server exits
func (s *schedulerAPI) Run(ctx context.Context) error {
	s.logger.Info("HTTP server starting on :8080")

	// Run server in a goroutine
	serverErr := make(chan error, 1)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	// Wait for either:
	// 1. context cancellation → trigger shutdown
	// 2. server error → return immediately
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Errorf("HTTP server shutdown error: %v", err)
			return err
		}
		return nil

	case err := <-serverErr:
		return err
	}
}

// ============================================================================
// Health & Status Handlers
// ============================================================================

func (s *schedulerAPI) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func (s *schedulerAPI) handleGetStatus(c *gin.Context) {
	status := gin.H{}

	currentID, err := s.scheduler.CurrentCommandID()
	if err == ErrNoActiveCommand {
		status["has_running_command"] = false
	} else {
		status["current_command_id"] = currentID
		status["has_running_command"] = true
	}

	c.JSON(http.StatusOK, status)
}

// ============================================================================
// Command Handlers
// ============================================================================

func (s *schedulerAPI) handleEnqueueCommand(c *gin.Context) {
	var req struct {
		Payload  string `json:"payload" binding:"required"`
		Priority uint   `json:"priority"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	command, err := s.scheduler.EnqueueCommand(&api.Command{
		Priority: req.Priority,
		Payload:  req.Payload,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, command)
}

func (s *schedulerAPI) handleGetCommand(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	command, err := s.scheduler.Store().LoadCommand(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, command)
}

func (s *schedulerAPI) handleGetCurrentCommand(c *gin.Context) {
	commandID, err := s.scheduler.CurrentCommandID()
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no command is currently running"})
		return
	}

	cmd, err := s.scheduler.Store().LoadCommand(commandID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if cmd == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		return
	}

	c.JSON(http.StatusOK, cmd)
}

func (s *schedulerAPI) handleListCommands(c *gin.Context) {
	ids, err := s.scheduler.Store().ListAllCommandIDs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	commands := make([]*api.Command, 0, len(ids))
	for _, id := range ids {
		cmd, err := s.scheduler.Store().LoadCommand(id)
		if err != nil || cmd == nil {
			continue
		}
		commands = append(commands, cmd)
	}

	c.JSON(http.StatusOK, commands)
}

func (s *schedulerAPI) handleDequeueCommand(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	if err := s.scheduler.PauseCommand(id); err != nil {
		// More informative error responses if you like:
		switch {
		case errors.Is(err, ErrCommandNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
			return
		case errors.Is(err, ErrCommandFinished):
			c.JSON(http.StatusConflict, gin.H{"error": "command is finished"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "command dequeued successfully"})
}

func (s *schedulerAPI) handleRequeueCommand(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	command, err := s.scheduler.RequeueCommand(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, command)
}

func (s *schedulerAPI) handleSetPriority(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	var req struct {
		Priority uint `json:"priority" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Load command
	cmd, err := s.scheduler.Store().LoadCommand(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cmd == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		return
	}

	// Update priority
	cmd.Priority = req.Priority

	if err := s.scheduler.Store().UpdateCommand(cmd); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to update command priority",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "priority updated",
		"command": cmd,
	})
}

// ============================================================================
// Task Handlers
// ============================================================================

func (s *schedulerAPI) handleListAllTasks(c *gin.Context) {
	// 1. List all command IDs
	ids, err := s.scheduler.Store().ListAllCommandIDs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	allTasks := make([]*api.Task, 0)

	// 2. Load each command and collect tasks
	for _, id := range ids {
		cmd, err := s.scheduler.Store().LoadCommand(id)
		if err != nil {
			// If a command was deleted in between, skip silently
			continue
		}
		if cmd == nil {
			continue
		}

		// Load each task
		for _, task := range cmd.Tasks {
			// Copy task to avoid exposing internal pointers
			taskCopy := task
			allTasks = append(allTasks, &taskCopy)
		}
	}

	// 3. Return all tasks
	c.JSON(http.StatusOK, allTasks)
}

func (s *schedulerAPI) handleListCommandTasks(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	cmd, err := s.scheduler.Store().LoadCommand(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cmd == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		return
	}

	tasks := make([]*api.Task, 0, len(cmd.Tasks))
	for _, t := range cmd.Tasks {
		tCopy := t
		tasks = append(tasks, &tCopy)
	}

	c.JSON(http.StatusOK, tasks)
}

// ============================================================================
// Helper Functions
// ============================================================================

func parseCommandID(c *gin.Context) (uint, error) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint(id), nil
}
