package serve

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/dioptra-io/ufuk-research/internal/nodes/ingest/v1"
	"github.com/dioptra-io/ufuk-research/internal/signal"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var logger = log.GetLogger()

// ServerContext holds the MPAT instance and other server dependencies
type ServerContext struct {
	mpat       mpat.MPAT
	mpatCtx    context.Context
	mpatCancel context.CancelFunc
}

func ServeCmd() *cobra.Command {
	var dbPath string

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the server",
		Long:  "Run the server that takes jobs and executes it.",
		Run: func(cmd *cobra.Command, args []string) {
			serveCmdRun(cmd, args, dbPath)
		},
	}

	serveCmd.Flags().StringVar(&dbPath, "db", "mpat.db", "Path to SQLite database")

	return serveCmd
}

func serveCmdRun(cmd *cobra.Command, args []string, dbPath string) {
	// Get debug flag value
	debug, _ := cmd.Flags().GetBool("debug")

	// Set Gin mode based on debug flag
	if debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	logger.Debugln("Starting the server.")

	// Initialize MPAT
	m, err := mpat.NewMPAT(dbPath)
	if err != nil {
		logger.Fatalf("Failed to create MPAT: %v", err)
	}

	// TODO: Register your nodes here
	if err := ingest.RegisterSelf(m); err != nil {
		logger.Fatalf("Failed to register node: %v", err)
	}

	// Freeze dependencies and load existing commands
	if err := m.FreezeDeps(true); err != nil {
		logger.Fatalf("Failed to freeze dependencies: %v", err)
	}

	// Create context for MPAT execution loop
	mpatCtx, mpatCancel := context.WithCancel(context.Background())

	// Start MPAT execution loop
	if err := m.Start(mpatCtx); err != nil {
		logger.Fatalf("Failed to start MPAT: %v", err)
	}

	serverCtx := &ServerContext{
		mpat:       m,
		mpatCtx:    mpatCtx,
		mpatCancel: mpatCancel,
	}

	router := setupRouter(serverCtx)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		logger.Println("Server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("listen: %s\n", err)
		}
	}()

	stopCh := signal.SetupSignalHandler()
	<-stopCh
	logger.Println("Shutdown signal received, initiating graceful shutdown...")

	// Cancel MPAT context first (stops accepting new tasks)
	logger.Println("Cancelling MPAT context...")
	mpatCancel()

	// Create a channel to signal when MPAT stops
	mpatStopDone := make(chan error, 1)
	go func() {
		logger.Println("Waiting for MPAT to stop...")
		mpatStopDone <- m.Stop()
	}()

	// Wait for MPAT to stop gracefully or timeout
	shutdownTimeout := 30 * time.Second
	select {
	case err := <-mpatStopDone:
		if err != nil {
			logger.Errorf("Error stopping MPAT: %v", err)
		} else {
			logger.Println("MPAT stopped successfully")
		}
	case <-time.After(shutdownTimeout):
		logger.Warnf("MPAT did not stop within %v, forcing shutdown", shutdownTimeout)
	}

	// Now shutdown the HTTP server
	logger.Println("Shutting down HTTP server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Errorf("Server forced to shutdown: %v", err)
	} else {
		logger.Println("HTTP server stopped successfully")
	}

	logger.Println("Server exited cleanly")
}

func setupRouter(serverCtx *ServerContext) *gin.Engine {
	r := gin.Default()

	// Health check
	r.GET("/hello", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"message": "hello",
		})
	})

	// MPAT API endpoints
	api := r.Group("/api/v1")
	{
		// Commands
		api.GET("/commands", serverCtx.listCommands)
		api.POST("/commands", serverCtx.enqueueCommand)
		api.GET("/commands/:id", serverCtx.getCommand)
		api.DELETE("/commands/:id", serverCtx.dequeueCommand)
		api.POST("/commands/:id/requeue", serverCtx.requeueCommand)
		api.PUT("/commands/:id/priority", serverCtx.setPriority)
		api.GET("/commands/current", serverCtx.getCurrentCommand)

		// Tasks
		api.GET("/tasks", serverCtx.listAllTasks)
		api.GET("/commands/:id/tasks", serverCtx.listCommandTasks)

		// Status
		api.GET("/status", serverCtx.getStatus)
	}

	return r
}

// Handler: Enqueue a new command
func (s *ServerContext) enqueueCommand(ctx *gin.Context) {
	var req struct {
		Params   string `json:"params" binding:"required"`
		Priority uint   `json:"priority"`
	}

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	command, err := s.mpat.EnqueueCommand(req.Params, req.Priority)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, command)
}

// Handler: Get command by ID
func (s *ServerContext) getCommand(ctx *gin.Context) {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	command, err := s.mpat.GetCommand(uint(id))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, command)
}

// Handler: Dequeue a command
func (s *ServerContext) dequeueCommand(ctx *gin.Context) {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	if err := s.mpat.DequeueCommand(uint(id)); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "command dequeued"})
}

// Handler: Requeue a command
func (s *ServerContext) requeueCommand(ctx *gin.Context) {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	command, err := s.mpat.RequeueCommand(uint(id))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, command)
}

// Handler: Set command priority
func (s *ServerContext) setPriority(ctx *gin.Context) {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	var req struct {
		Priority uint `json:"priority" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.mpat.SetPriority(uint(id), req.Priority); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "priority updated"})
}

// Handler: Get current running command
func (s *ServerContext) getCurrentCommand(ctx *gin.Context) {
	commandID, err := s.mpat.GetCurrentCommandID()
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	command, err := s.mpat.GetCommand(commandID)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, command)
}

// Handler: Get server status
func (s *ServerContext) getStatus(ctx *gin.Context) {
	currentID, err := s.mpat.GetCurrentCommandID()
	hasRunning := err == nil

	status := gin.H{
		"dependencies_frozen": s.mpat.AreDepsFrozen(),
		"has_running_task":    hasRunning,
	}

	if hasRunning {
		status["current_command_id"] = currentID
	}

	ctx.JSON(http.StatusOK, status)
}

// Handler: List all commands
func (s *ServerContext) listCommands(ctx *gin.Context) {
	commands, err := s.mpat.ListCommands()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, commands)
}

// Handler: List all tasks across all commands
func (s *ServerContext) listAllTasks(ctx *gin.Context) {
	tasks, err := s.mpat.ListAllTasks()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, tasks)
}

// Handler: List tasks for a specific command
func (s *ServerContext) listCommandTasks(ctx *gin.Context) {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	tasks, err := s.mpat.ListTasksForCommand(uint(id))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, tasks)
}
