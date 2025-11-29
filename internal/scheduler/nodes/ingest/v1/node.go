package ingest

import (
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

func onTaskCreate(command *api.Command, task *api.Task) error {
	logger.Debugln("create function is invoked")
	return nil
}

func onTaskStarted(command *api.Command, task *api.Task) error {
	logger.Debugln("start function is invoked")

	time.Sleep(10 * time.Second)
	return nil
}

func onTaskRestarted(command *api.Command, task *api.Task) error {
	logger.Debugln("restart function is invoked")
	return nil
}

func onExit() {
	logger.Debugln("exit function is invoked")
}
