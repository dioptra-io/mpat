package scheduler

import (
	"context"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

// Node is also known as a processing Node. It defines a name and some operations. It is a generalization. In MPAT it is
// used to ingress data from different sources, or to run chunked ClickHouse queries to generate database tables.
type Node interface {
	// Returns the named version of the node, this is used for tracking the implementation name and version.
	NamedVersion() api.NamedVersion

	// Returns the dependencies.
	Dependencies() []api.NamedVersion

	// Return the event and error channel for the node. This is how the scheduler waits for the node to process.
	CommChan() (chan api.Event, chan error)
}

// This is a templateNode that can be used for testing.
type templateNode struct {
	cfg       *NodeConfig
	eventChan chan api.Event
	errChan   chan error
	ctx       context.Context
}

type EventHandleFn func(ctx context.Context, command *api.Command, task *api.Task) error

type ExitHandler func()

type NodeConfig struct {
	// Name and version.
	Name    string
	Version uint

	// List of dependencies
	Dependencies []api.NamedVersion

	// Channle size of the event and error channel. Default is 1.
	ChanLength int

	// This is invoken when Task is created.
	OnTaskCreated EventHandleFn

	// This is invoken when Task is started.
	OnTaskStarted EventHandleFn

	// This is invoken when Task is restarted.
	OnTaskRestarted EventHandleFn

	// This is invoken when exit signal is received. Can be nil.
	OnExit ExitHandler
}

// This will create a new node and starts a separate go routine for the handlers.
func SpawnNode(cfg *NodeConfig) Node {
	ctx, cancel := context.WithCancel(context.Background())

	m := &templateNode{
		cfg:       cfg,
		eventChan: make(chan api.Event, cfg.ChanLength),
		errChan:   make(chan error, cfg.ChanLength),
		ctx:       ctx,
	}

	go func() {
		defer close(m.errChan)
		defer close(m.eventChan)
		defer cancel()

		// This needs some adjustments as the context cannot be cancelled before the next event is
		// processed. TODO.
		for event := range m.eventChan {
			switch event.EventType {
			case api.OnTaskCreated:
				m.errChan <- cfg.OnTaskCreated(ctx, event.Command, event.Task)

			case api.OnTaskStarted:
				m.errChan <- cfg.OnTaskStarted(ctx, event.Command, event.Task)

			case api.OnTaskRestarted:
				m.errChan <- cfg.OnTaskRestarted(ctx, event.Command, event.Task)

			case api.OnSchedulerExit:
				cancel()
				if cfg.OnExit != nil {
					cfg.OnExit()
				}
				return // Exit the loop
			}
		}
	}()

	return m
}

func (n *templateNode) NamedVersion() api.NamedVersion {
	return api.NewNV(n.cfg.Name, n.cfg.Version)
}

func (n *templateNode) Dependencies() []api.NamedVersion {
	return n.cfg.Dependencies
}

func (n *templateNode) CommChan() (chan api.Event, chan error) {
	return n.eventChan, n.errChan
}
