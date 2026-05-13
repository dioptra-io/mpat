package workers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

// InvokeWorker is expected to return the ErrTaskCancelled error if the cause of
// the context cancellation is ErrTaskCancelled.
func InvokeWorker(ctx context.Context, task *api.Task, workerId int, logger *slog.Logger) error {
	switch task.Type() {
	case api.TaskTypeRetinaStream:
		// handle accordingle
		if err := retinaStream(ctx, task, workerId, logger); err != nil {
			// handle the cancellation.
		}
	default:
		return fmt.Errorf("unknown type for the task: %s", task.Type())
	}

	return nil
}
