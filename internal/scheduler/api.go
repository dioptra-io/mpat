package scheduler

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/gin-gonic/gin"
)

// SchedulerAPI wraps the scheduler with HTTP API capabilities
type SchedulerAPI interface {
	// SetupHandlers configures HTTP routes on the provided router
	SetupHandlers(router *gin.Engine)

	// Start starts the HTTP server in a goroutine
	// Returns error channel that signals when HTTP server exits
	Start(ctx context.Context) chan error
}

type schedulerAPI struct {
	scheduler Scheduler
	server    *http.Server
	router    *gin.Engine
}

// NewSchedulerAPI creates a new SchedulerAPI wrapper around a scheduler
func NewSchedulerAPI(sched Scheduler) SchedulerAPI {
	return &schedulerAPI{
		scheduler: sched,
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

// Start starts the HTTP server in a goroutine
// Returns error channel that signals when HTTP server exits
func (s *schedulerAPI) Start(ctx context.Context) chan error {
	errChan := make(chan error, 1)

	go func() {
		logger.Info("HTTP server starting on :8080")
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorf("HTTP server error: %v", err)
			errChan <- err
			return
		}
		errChan <- nil
	}()

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			logger.Errorf("HTTP server shutdown error: %v", err)
		} else {
			logger.Info("Exiting HTTP server")
		}
	}()

	return errChan
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
	currentID, err := s.scheduler.GetCurrentCommandID()

	status := gin.H{
		"has_running_command": err == nil && currentID != nil,
	}

	if currentID != nil {
		status["current_command_id"] = *currentID
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

	command, err := s.scheduler.EnqueueCommand(req.Payload, req.Priority)
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

	command, err := s.scheduler.GetCommand(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, command)
}

func (s *schedulerAPI) handleGetCurrentCommand(c *gin.Context) {
	commandID, err := s.scheduler.GetCurrentCommandID()
	if err != nil || commandID == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no command is currently running"})
		return
	}

	command, err := s.scheduler.GetCommand(*commandID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, command)
}

func (s *schedulerAPI) handleListCommands(c *gin.Context) {
	commands, err := s.scheduler.ListCommands()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, commands)
}

func (s *schedulerAPI) handleDequeueCommand(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	if err := s.scheduler.DequeueCommand(id); err != nil {
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

	if err := s.scheduler.SetPriority(id, req.Priority); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "priority updated successfully"})
}

// ============================================================================
// Task Handlers
// ============================================================================

func (s *schedulerAPI) handleListAllTasks(c *gin.Context) {
	commands, err := s.scheduler.ListCommands()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Collect all tasks from all commands
	allTasks := make([]*api.Task, 0)
	for _, cmd := range commands {
		for _, task := range cmd.Tasks {
			taskCopy := *task
			allTasks = append(allTasks, &taskCopy)
		}
	}

	c.JSON(http.StatusOK, allTasks)
}

func (s *schedulerAPI) handleListCommandTasks(c *gin.Context) {
	id, err := parseCommandID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	tasks, err := s.scheduler.ListTasksForCommand(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
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
