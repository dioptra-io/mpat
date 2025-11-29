package serve

import (
	"context"
	"sync"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/dioptra-io/ufuk-research/internal/scheduler"
	ingestv1 "github.com/dioptra-io/ufuk-research/internal/scheduler/nodes/ingest/v1"
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
			serveCmdRun(dbPath)
		},
	}

	serveCmd.Flags().StringVar(&dbPath, "db", ":memory:", "Path to SQLite database")

	gin.SetMode(gin.ReleaseMode)

	return serveCmd
}

func serveCmdRun(dbPath string) {
	logger.Debugln("Starting the server.")
	var err error
	var storeObject scheduler.Store

	// Create store
	storeObject, err = scheduler.NewSQLiteStore(dbPath)

	if err != nil {
		logger.Fatalf("Failed to create store: %v", err)
	}

	// Add the processing nodes here with the topological order.
	sched, err := scheduler.NewScheduler(storeObject, logger,
		ingestv1.NewIngestNode(),
	)
	if err != nil {
		logger.Fatalf("Failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add the processing nodes here with the topological order.
	schedAPI := scheduler.NewSchedulerAPI(sched, logger)
	schedAPI.SetupHandlers(gin.Default())

	var wg sync.WaitGroup
	wg.Add(2)

	// --- 1. Run Scheduler ---
	go func() {
		defer wg.Done()
		if err := sched.Run(ctx); err != nil && err != context.Canceled {
			logger.Errorf("Scheduler exited with error: %v", err)
		} else {
			logger.Infoln("Scheduler stopped cleanly")
		}
	}()

	// --- 2. Run HTTP Server ---
	go func() {
		defer wg.Done()
		if err := schedAPI.Run(ctx); err != nil {
			logger.Errorf("HTTP API exited with error: %v", err)
		} else {
			logger.Infoln("HTTP API stopped cleanly")
		}
	}()

	// Wait for shutdown signal
	logger.Infoln("Scheduler running. Press Ctrl+C to shutdown")
	<-signal.SetupSignalHandler()
	logger.Infoln("Shutdown signal received")

	// Cancel scheduler context
	cancel()

	// Wait for both goroutines to exit
	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		logger.Infoln("All services stopped")
	case <-time.After(10 * time.Second):
		logger.Warn("Timeout waiting for services to stop")
	}
}
