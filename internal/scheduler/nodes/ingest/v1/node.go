package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/scheduler"
)

var logger = log.GetLogger()

func NewIngestNode() scheduler.Node {
	return scheduler.SpawnNode(&scheduler.NodeConfig{
		Name:            "ingest_node",
		Version:         1,
		Dependencies:    []api.NamedVersion{},
		ChanLength:      10,
		OnTaskCreated:   onTaskCreate,
		OnTaskStarted:   onTaskStarted,
		OnTaskRestarted: onTaskRestarted,
		OnExit:          onExit,
	})
}

func onTaskCreate(ctx context.Context, command *api.Command, task *api.Task) error {
	logger.Debugln("create function is invoked")

	return nil
}

func onTaskStarted(ctx context.Context, command *api.Command, task *api.Task) error {
	logger.Debugln("start function is invoked")

	time.Sleep(10 * time.Second)
	return errors.New("this is an error")
}

func onTaskRestarted(ctx context.Context, command *api.Command, task *api.Task) error {
	logger.Debugln("restart function is invoked")
	return nil
}

func onExit() {
	logger.Debugln("exit function is invoked")
}
