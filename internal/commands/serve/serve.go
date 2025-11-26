package serve

import (
	"context"
	"net/http"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/signal"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var logger = log.GetLogger()

func ServeCmd() *cobra.Command {
	metricsCmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the server",
		Long:  "Run the server that takes jobs and executes it.",
		Run:   serveCmdRun,
	}

	return metricsCmd
}

func serveCmdRun(cmd *cobra.Command, args []string) {
	logger.Debugln("Starting the server.")

	router := setupRouter()
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
	logger.Println("Shutdown signal received")

	// Graceful shutdown timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Fatalf("Server forced to shutdown: %v", err)
	}

	logger.Println("Server exited cleanly")
}

func setupRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/hello", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"message": "hello",
		})
	})

	return r
}
