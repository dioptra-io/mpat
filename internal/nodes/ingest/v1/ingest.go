package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/api"
	"github.com/dioptra-io/ufuk-research/internal/log"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
)

var logger = log.GetLogger()

// ingestNode implements the mpat.Node interface (private)
type ingestNode struct {
	name    string
	version uint
}

// newIngestNode creates a new ingest node (private)
func newIngestNode() *ingestNode {
	return &ingestNode{
		name:    "ingest",
		version: 1,
	}
}

// RegisterSelf registers the ingest node with MPAT
func RegisterSelf(m mpat.MPAT) error {
	node := newIngestNode()
	logger.Infof("Registering node: %s v%d", node.name, node.version)

	// Register with no dependencies (root node)
	if err := m.RegisterNode(node); err != nil {
		return fmt.Errorf("failed to register ingest node: %w", err)
	}

	logger.Infof("Successfully registered node: %s v%d", node.name, node.version)
	return nil
}

// NamedVersion returns the node's name and version
func (n *ingestNode) NamedVersion() api.NamedVersion {
	return api.NamedVersion{
		Name:    n.name,
		Version: n.version,
	}
}

// GetDefaultTaskParams returns default parameters for a task
func (n *ingestNode) GetDefaultTaskParams(c *api.Command) string {
	return fmt.Sprintf(`{"node": "%s", "version": %d, "created_at": "%s"}`,
		n.name, n.version, time.Now().Format(time.RFC3339))
}

// OnTaskRun executes the main task logic
func (n *ingestNode) OnTaskRun(ctx context.Context, c *api.Command, t *api.Task) error {
	logger.Infof("[Ingest] Starting task %d for command %d", t.ID, c.ID)
	logger.Infof("[Ingest] Command params: %s", c.Params)
	logger.Infof("[Ingest] Task params: %s", t.Params)

	// Simulate ingestion work with context awareness
	for i := range 10 {
		select {
		case <-ctx.Done():
			logger.Warnf("[Ingest] Task %d cancelled at step %d/%d", t.ID, i+1, 10)
			return ctx.Err()
		default:
			logger.Infof("[Ingest] Task %d progress: %d/10", t.ID, i+1)
			time.Sleep(500 * time.Millisecond)
		}
	}

	logger.Infof("[Ingest] Completed task %d for command %d", t.ID, c.ID)
	return nil
}

// OnTaskLoad is called when a task is loaded during requeue
func (n *ingestNode) OnTaskLoad(ctx context.Context, c *api.Command, t *api.Task, prevState api.Status) error {
	logger.Infof("[Ingest] Loading task %d (previous state: %s)", t.ID, prevState)

	// Handle different previous states
	switch prevState {
	case api.StatusRunning:
		logger.Warnf("[Ingest] Task %d was running, may need cleanup", t.ID)
	case api.StatusFailed:
		logger.Warnf("[Ingest] Task %d previously failed, resetting", t.ID)
	case api.StatusDone:
		logger.Infof("[Ingest] Task %d already completed", t.ID)
	}

	return nil
}

// OnTaskCreate is called when a new task is created
func (n *ingestNode) OnTaskCreate(ctx context.Context, c *api.Command, t *api.Task, enqueue bool) error {
	action := "requeue"
	if enqueue {
		action = "enqueue"
	}

	logger.Infof("[Ingest] Creating task %d (action: %s)", t.ID, action)
	logger.Debugf("[Ingest] Task parameters: %s", t.Params)

	// You could do initialization here, like:
	// - Validate command parameters
	// - Allocate resources
	// - Create workspace directories

	return nil
}

// OnTaskStopped is called when an idle task is dequeued
func (n *ingestNode) OnTaskStopped(ctx context.Context, c *api.Command, t *api.Task) error {
	logger.Infof("[Ingest] Task %d stopped (was idle)", t.ID)

	// Cleanup any resources allocated for this task
	// - Delete temporary files
	// - Release locks
	// - Clean up state

	return nil
}

// OnTaskInterrupted is called when a running task is dequeued
func (n *ingestNode) OnTaskInterrupted(ctx context.Context, c *api.Command, t *api.Task) error {
	logger.Warnf("[Ingest] Task %d interrupted (was running)", t.ID)

	// More aggressive cleanup for interrupted tasks
	// - Save partial progress
	// - Cancel ongoing operations
	// - Clean up resources

	return nil
}
