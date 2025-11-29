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

	gin.SetMode(gin.ReleaseMode)

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

	// Add the processing nodes here with the topological order.
	schedAPI := scheduler.NewSchedulerAPI(sched)
	schedAPI.SetupHandlers(gin.Default())

	// Start scheduler execution loop
	schedulerErrChan := sched.Start(schedulerCtx)
	schedAPIErrChan := schedAPI.Start(schedulerCtx)

	// Wait for shutdown signal
	logger.Infoln("Scheduler running. Press Ctrl+C to shutdown")
	<-signal.SetupSignalHandler()

	logger.Infoln("Shutdown signal received, shutting down all services")

	// Cancel scheduler context
	schedulerCancelFn()

	// Wait for scheduler to finish gracefully
	waitForServices(schedulerErrChan, schedAPIErrChan, 10*time.Second)
}

func waitForServices(schedulerChan, apiChan chan error, timeout time.Duration) {
	done := make(chan struct{})

	go func() {
		// Wait for scheduler
		if err := <-schedulerChan; err != nil {
			logger.Errorf("Scheduler exited with error: %v", err)
		} else {
			logger.Info("Exitting scheduler.")
		}

		// Wait for API server
		if err := <-apiChan; err != nil {
			logger.Errorf("Exiting HTTP server with error: %v", err)
		}

		close(done)
	}()

	select {
	case <-done:
		logger.Info("Exiting main go routine")
	case <-time.After(timeout):
		logger.Warn("Exiting main go routine timeout, shutting down")
	}

	time.Sleep(10 * time.Millisecond)
}
