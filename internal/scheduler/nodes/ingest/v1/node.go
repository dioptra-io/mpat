package ingest

import (
	"context"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/scheduler"
)

var logger = log.GetLogger()

// This is a IngestNode that can be used for testing.
type IngestNode struct {
	// etc.
}

func NewIngestNode() scheduler.Node {
	return &IngestNode{}
}

func (n *IngestNode) NamedVersion() api.NamedVersion {
	return api.NewNV("ingest_node", 1) // Hardcoded name and version
}

func (n *IngestNode) OnEvent(ctx context.Context, c *api.Command, t *api.Task, event api.Event) error {
	switch event {
	case api.CommandCreated:
		logger.Infof("Command created: %v\n", c.Params)
	}
	return nil
}
