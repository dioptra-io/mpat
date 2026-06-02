package cmd

import (
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
	"github.com/spf13/cobra"
)

var (
	retinaStreamEndpoint string
	retinaStreamDuration time.Duration
)

var newCmd = &cobra.Command{
	Use:   "new",
	Short: "Create a new task",
}

var newRetinaStreamCmd = &cobra.Command{
	Use:   "retina_stream",
	Short: "Create a new retina_stream",
	RunE: func(cmd *cobra.Command, args []string) error {
		if retinaStreamEndpoint == "" {
			return fmt.Errorf("--endpoint cannot be empty")
		}
		durationSeconds := int64(retinaStreamDuration.Seconds())

		task := api.Task{
			RetinaStream: &api.RetinaStreamTaskRequest{
				Endpoint:        retinaStreamEndpoint,
				DurationSeconds: durationSeconds,
				OutputFile:      retinaDBName(durationSeconds),
			},
		}

		client := mpat.NewClient(addr)
		createdTask, err := client.CreateTask(cmd.Context(), task)
		if err != nil {
			return err
		}
		fmt.Println(createdTask.UUID)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(newCmd)
	newCmd.AddCommand(newRetinaStreamCmd)

	newRetinaStreamCmd.Flags().StringVar(&retinaStreamEndpoint, "endpoint", "https://iprl.dioptra.io/api/v1/stream", "Retina endpoint to measure")
	newRetinaStreamCmd.Flags().DurationVar(&retinaStreamDuration, "duration", time.Second*10, "measurement duration")
}

func retinaDBName(durationSeconds int64) string {
	return fmt.Sprintf("%s__stream_retina__%ds.db", time.Now().Format("20060102150405"), durationSeconds)
}
