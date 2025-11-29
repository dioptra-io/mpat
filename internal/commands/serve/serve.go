package serve

import (
	"context"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/dioptra-io/ufuk-research/internal/scheduler"
	ingestv1 "github.com/dioptra-io/ufuk-research/internal/scheduler/nodes/ingest/v1"
	"github.com/dioptra-io/ufuk-research/internal/scheduler/store"
	"github.com/dioptra-io/ufuk-research/internal/signal"
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
	logger.Debugln("Starting the server.")

	// Create store
	store, err := store.NewInMemoryStore()
	if err != nil {
		logger.Fatalf("Failed to create store: %v", err)
	}

	// Add the processing nodes here with the topological order.
	sched, err := scheduler.NewScheduler(store,
		ingestv1.NewIngestNode(),
	)
	if err != nil {
		logger.Fatalf("Failed to create scheduler: %v", err)
	}

	// Create context for scheduler execution loop
	schedulerCtx, schedulerCancelFn := context.WithCancel(context.Background())
	defer schedulerCancelFn() // Ensure cleanup

	// Start scheduler execution loop
	schedulerErrChan := sched.Start(schedulerCtx)

	// Uncomment when ready to add HTTP server
	// serverCtx := &ServerContext{
	// 	scheduler:       sched,
	// 	schedulerCtx:    schedulerCtx,
	// 	schedulerCancel: schedulerCancelFn,
	// }
	//
	// router := setupRouter(serverCtx)
	//
	// srv := &http.Server{
	// 	Addr:    ":8080",
	// 	Handler: router,
	// }
	//
	// // Start HTTP server
	// go func() {
	// 	logger.Println("Server starting on :8080")
	// 	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	// 		logger.Fatalf("HTTP server error: %s\n", err)
	// 	}
	// }()

	// Wait for shutdown signal
	logger.Infoln("Scheduler running. Press Ctrl+C to shutdown...")
	<-signal.SetupSignalHandler()

	logger.Infoln("Shutdown signal received, shutting down.")

	// Cancel scheduler context
	schedulerCancelFn()

	// Uncomment when HTTP server is active
	// // Shutdown HTTP server with timeout
	// shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer shutdownCancel()
	//
	// logger.Println("Shutting down HTTP server...")
	// if err := srv.Shutdown(shutdownCtx); err != nil {
	// 	logger.Warnf("HTTP server shutdown error: %v", err)
	// }

	// Wait for scheduler to finish gracefully
	logger.Println("Waiting for scheduler to exit...")
	select {
	case err := <-schedulerErrChan:
		if err != nil {
			logger.Errorf("Scheduler exited with error: %v", err)
		} else {
			logger.Info("Scheduler exited without error.")
		}
	case <-time.After(10 * time.Second):
		logger.Warn("Scheduler did not exit within timeout, forcing shutdown")
	}
}

// func setupRouter(serverCtx *ServerContext) *gin.Engine {
// 	r := gin.Default()
//
// 	// Health check
// 	r.GET("/hello", func(ctx *gin.Context) {
// 		ctx.JSON(http.StatusOK, gin.H{
// 			"message": "hello",
// 		})
// 	})
//
// 	// MPAT API endpoints
// 	api := r.Group("/api/v1")
// 	{
// 		// Commands
// 		api.GET("/commands", serverCtx.listCommands)
// 		api.POST("/commands", serverCtx.enqueueCommand)
// 		api.GET("/commands/:id", serverCtx.getCommand)
// 		api.DELETE("/commands/:id", serverCtx.dequeueCommand)
// 		api.POST("/commands/:id/requeue", serverCtx.requeueCommand)
// 		api.PUT("/commands/:id/priority", serverCtx.setPriority)
// 		api.GET("/commands/current", serverCtx.getCurrentCommand)
//
// 		// Tasks
// 		api.GET("/tasks", serverCtx.listAllTasks)
// 		api.GET("/commands/:id/tasks", serverCtx.listCommandTasks)
//
// 		// Status
// 		api.GET("/status", serverCtx.getStatus)
// 	}
//
// 	return r
// }
//
// // Handler: Enqueue a new command
// func (s *ServerContext) enqueueCommand(ctx *gin.Context) {
// 	var req struct {
// 		Params   string `json:"params" binding:"required"`
// 		Priority uint   `json:"priority"`
// 	}
//
// 	if err := ctx.ShouldBindJSON(&req); err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	command, err := s.mpat.EnqueueCommand(req.Params, req.Priority)
// 	if err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusCreated, command)
// }
//
// // Handler: Get command by ID
// func (s *ServerContext) getCommand(ctx *gin.Context) {
// 	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
// 	if err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
// 		return
// 	}
//
// 	command, err := s.mpat.GetCommand(uint(id))
// 	if err != nil {
// 		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, command)
// }
//
// // Handler: Dequeue a command
// func (s *ServerContext) dequeueCommand(ctx *gin.Context) {
// 	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
// 	if err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
// 		return
// 	}
//
// 	if err := s.mpat.DequeueCommand(uint(id)); err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, gin.H{"message": "command dequeued"})
// }
//
// // Handler: Requeue a command
// func (s *ServerContext) requeueCommand(ctx *gin.Context) {
// 	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
// 	if err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
// 		return
// 	}
//
// 	command, err := s.mpat.RequeueCommand(uint(id))
// 	if err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, command)
// }
//
// // Handler: Set command priority
// func (s *ServerContext) setPriority(ctx *gin.Context) {
// 	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
// 	if err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
// 		return
// 	}
//
// 	var req struct {
// 		Priority uint `json:"priority" binding:"required"`
// 	}
//
// 	if err := ctx.ShouldBindJSON(&req); err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	if err := s.mpat.SetPriority(uint(id), req.Priority); err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, gin.H{"message": "priority updated"})
// }
//
// // Handler: Get current running command
// func (s *ServerContext) getCurrentCommand(ctx *gin.Context) {
// 	commandID, err := s.mpat.GetCurrentCommandID()
// 	if err != nil {
// 		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	command, err := s.mpat.GetCommand(commandID)
// 	if err != nil {
// 		ctx.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, command)
// }
//
// // Handler: Get server status
// func (s *ServerContext) getStatus(ctx *gin.Context) {
// 	currentID, err := s.mpat.GetCurrentCommandID()
// 	hasRunning := err == nil
//
// 	status := gin.H{
// 		"dependencies_frozen": s.mpat.AreDepsFrozen(),
// 		"has_running_task":    hasRunning,
// 	}
//
// 	if hasRunning {
// 		status["current_command_id"] = currentID
// 	}
//
// 	ctx.JSON(http.StatusOK, status)
// }
//
// // Handler: List all commands
// func (s *ServerContext) listCommands(ctx *gin.Context) {
// 	commands, err := s.mpat.ListCommands()
// 	if err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, commands)
// }
//
// // Handler: List all tasks across all commands
// func (s *ServerContext) listAllTasks(ctx *gin.Context) {
// 	tasks, err := s.mpat.ListAllTasks()
// 	if err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, tasks)
// }
//
// // Handler: List tasks for a specific command
// func (s *ServerContext) listCommandTasks(ctx *gin.Context) {
// 	id, err := strconv.ParseUint(ctx.Param("id"), 10, 32)
// 	if err != nil {
// 		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
// 		return
// 	}
//
// 	tasks, err := s.mpat.ListTasksForCommand(uint(id))
// 	if err != nil {
// 		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 		return
// 	}
//
// 	ctx.JSON(http.StatusOK, tasks)
// }
