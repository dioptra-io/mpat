package scheduler

import (
	"context"
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/sirupsen/logrus"
)

// Node is also known as a processing Node. It defines a name and some operations. It is a generalization. In MPAT it is
// used to ingress data from different sources, or to run chunked ClickHouse queries to generate database tables.
type Node interface {
	// Returns the named version of the node, this is used for tracking the implementation name and version.
	NamedVersion() api.NamedVersion

	// This is invoked when a new event is emmitted for a command and task on this node.
	OnEvent(ctx context.Context, c *api.Command, t *api.Task, event api.Event) error
}

// This is a MockNode that can be used for testing.
type MockNode struct {
	nv     api.NamedVersion
	logger logrus.FieldLogger
}

func NewMockNode(name string, version uint, fieldLogger logrus.FieldLogger) Node {
	return &MockNode{
		nv:     api.NewNV(name, version),
		logger: fieldLogger,
	}
}

func (n *MockNode) NamedVersion() api.NamedVersion {
	return n.nv
}

func (n *MockNode) OnEvent(ctx context.Context, c *api.Command, t *api.Task, event api.Event) error {
	commandStr := ""
	taskStr := ""

	if c != nil {
		commandStr = fmt.Sprintf(" for Command %d\n", c.ID)
	}
	if t != nil {
		taskStr = fmt.Sprintf(" for Task %q\n", t.NodeNamedVersion.String())
	}

	if n.logger != nil {
		n.logger.Debugf("Event %s received%s%s.", event, commandStr, taskStr)
	}

	return nil
}
