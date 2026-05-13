package cmd

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/dioptra-io/ufuk-research/internal/store"
	"github.com/dioptra-io/ufuk-research/internal/worker"
	"github.com/spf13/cobra"
)

var (
	serveAddr  string
	numWorkers int
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the MPAT worker server",
	Long: `Start the MPAT worker runtime and HTTP API server.

The server accepts incoming tasks, queues them,
and processes them asynchronously using worker goroutines.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(
			context.Background(),
			syscall.SIGINT,
			syscall.SIGTERM,
		)
		defer cancel()

		// TODO:
		// Replace with real implementation later.
		var workerStore store.WorkerStore

		w, err := worker.NewWorkerFromConfig(worker.WorkerConfig{
			Addr:       serveAddr,
			NumWorkers: numWorkers,
			QueueSize:  1024,
		}, workerStore, logger)
		if err != nil {
			return err
		}

		slog.Info(
			"worker initialized",
			"addr", serveAddr,
			"num_workers", numWorkers,
		)

		return w.Run(ctx)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVar(
		&serveAddr,
		"addr",
		"localhost:9293",
		"http listen address",
	)

	serveCmd.Flags().IntVar(
		&numWorkers,
		"num-workers",
		1,
		"number of concurrent workers",
	)
}
