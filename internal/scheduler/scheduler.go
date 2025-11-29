package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/dioptra-io/ufuk-research/internal/log"
)

// This is the task scheduler. The usage is that the nodes are added and then the dependencies are frozen to perform the
// compuation. The commands can be added to the queue at any time. Each command would spawn tasks t run one by one. Note
// that the queue is designed to run a single task at a  time.
type Scheduler interface {
	// Adds a command to the queue with the given params and the priority.
	EnqueueCommand(params string, p uint) (*api.Command, error)

	// Removes the command from the queue, stops all its running tasks, marks them as waiting, marks the command as not
	// active.
	DequeueCommand(commandID uint) error

	// Adds the command to the queue again, marks its tasks are idle, marks the command as active.
	RequeueCommand(commandID uint) (*api.Command, error)

	// Starts the execution loop in a separate go routine, cancelling the context would stop execution.
	Start(ctx context.Context) chan error

	// Gets the current active command ID, if there are none returns an error.
	GetCurrentCommandID() (*uint, error)

	// Gets the command with the given ID, if it doesn't exists then returns an error.
	GetCommand(commandID uint) (*api.Command, error)

	// Sets the priority of a command, this would not stop the if a task of this command is running.
	SetPriority(commandID uint, p uint) error

	// ListCommands returns all commands
	ListCommands() ([]*api.Command, error)

	// ListTasksForCommand returns all tasks for a specific command
	ListTasksForCommand(commandID uint) ([]*api.Task, error)
}

var logger = log.GetLogger()

// Scheduler implementation
type scheduler struct {
	mu          sync.RWMutex
	store       Store
	queue       Queue
	nodes       map[api.NamedVersion]Node
	additiveDAG AdditiveDAG
	currentCmd  *uint // Pointer to allow nil (no current command)
	ctx         context.Context
}

// NewScheduler creates a new scheduler with the given store, queue, and nodes. It builds the dependency graph from the
// nodes. Nodes must be provided in topological order (dependencies before dependents).
func NewScheduler(store Store, nodes ...Node) (Scheduler, error) {
	if store == nil {
		return nil, fmt.Errorf("store cannot be nil")
	}

	s := &scheduler{
		store:       store,
		queue:       NewLinkedQueue(),
		nodes:       make(map[api.NamedVersion]Node),
		additiveDAG: NewAdditiveDAG(),
	}

	// Add all nodes to the map and DSG
	for _, node := range nodes {
		nv := node.NamedVersion()
		deps := node.Dependencies()

		for _, depNV := range deps {
			if _, ok := s.nodes[depNV]; !ok {
				return nil, fmt.Errorf("node %s depends on %s which has not been added yet (nodes must be in topological order)", nv.String(), depNV.String())
			}
		}
		s.nodes[nv] = node

		// Add node to DSG
		if err := s.additiveDAG.AddNode(node, deps...); err != nil {
			return nil, fmt.Errorf("failed to add node %s to DSG: %w", nv.String(), err)
		}
	}

	return s, nil
}

func (s *scheduler) GetCurrentCommandID() (*uint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.currentCmd, nil
}

func (s *scheduler) GetCommand(commandID uint) (*api.Command, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

func (s *scheduler) SetPriority(commandID uint, p uint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the command
	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		return err
	}

	// Update priority
	cmd.Priority = p
	if err := s.store.SaveCommand(cmd); err != nil {
		return fmt.Errorf("failed to update command priority: %w", err)
	}

	return nil
}

func (s *scheduler) ListCommands() ([]*api.Command, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	commandIDs, err := s.store.GetAllCommandIDs()
	if err != nil {
		return nil, fmt.Errorf("failed to load commands: %w", err)
	}

	commands := make([]*api.Command, 0, len(commandIDs))
	for _, commandID := range commandIDs {
		command, err := s.store.LoadCommand(commandID)
		if err != nil {
			return nil, fmt.Errorf("failed to load command: %w", err)
		}

		commands = append(commands, command)
	}

	return commands, nil
}

func (s *scheduler) ListTasksForCommand(commandID uint) ([]*api.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	command, err := s.store.LoadCommand(commandID)
	if err != nil {
		return nil, fmt.Errorf("failed to find tasks for command %d: %w", commandID, err)
	}

	tasks := make([]*api.Task, 0, len(command.Tasks))
	for _, task := range command.Tasks {
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (s *scheduler) EnqueueCommand(params string, p uint) (*api.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a new command in the store
	cmd, err := s.store.CreateCommand(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create command: %w", err)
	}

	// Set priority and status
	cmd.Priority = p
	cmd.Status = api.CommandStatusActive

	// Save the updated command
	if err := s.store.SaveCommand(cmd); err != nil {
		return nil, fmt.Errorf("failed to save command: %w", err)
	}

	// Add to queue with priority
	if err := s.queue.Enqueue(s.ctx, cmd.ID, p); err != nil {
		return nil, fmt.Errorf("failed to enqueue command: %w", err)
	}

	return cmd, nil
}

func (s *scheduler) DequeueCommand(commandID uint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load the command
	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		return fmt.Errorf("failed to load command: %w", err)
	}

	// Check if command is finished
	if cmd.IsFinished() {
		return fmt.Errorf("cannot deactivate finished command %d", commandID)
	}

	// Set status to inactive
	cmd.Status = api.CommandStatusInactive

	// Save the updated command
	if err := s.store.SaveCommand(cmd); err != nil {
		return fmt.Errorf("failed to save command: %w", err)
	}

	// Remove the commandID from queue.
	if err := s.queue.Remove(s.ctx, commandID); err != nil {
		return fmt.Errorf("failed to enqueue command: %w", err)
	}

	return nil
}

func (s *scheduler) RequeueCommand(commandID uint) (*api.Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load the command
	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		return nil, fmt.Errorf("failed to load command: %w", err)
	}

	// Check if command is finished
	if cmd.IsFinished() {
		return nil, fmt.Errorf("cannot requeue finished command %d", commandID)
	}

	// Set status to active
	cmd.Status = api.CommandStatusActive

	// Save the updated command
	if err := s.store.SaveCommand(cmd); err != nil {
		return nil, fmt.Errorf("failed to save command: %w", err)
	}

	// Add to queue with its priority
	if err := s.queue.Enqueue(s.ctx, cmd.ID, cmd.Priority); err != nil {
		return nil, fmt.Errorf("failed to enqueue command: %w", err)
	}

	return cmd, nil
}

func (s *scheduler) Start(ctx context.Context) chan error {
	schedulerErrChan := make(chan error, 1)
	//  the execution loop in a goroutine
	go func() {
		// Save the context.
		s.ctx = ctx

		// Load the existing commandIDs first and add them to the queue.
		defer s.sendCloseSignals(schedulerErrChan)

		commandIDs, err := s.store.GetAllCommandIDs()
		if err != nil {
			logger.Errorf("Failed to load the command: %s", err)
			return
		}

		for _, commandID := range commandIDs {
			if _, err := s.RequeueCommand(commandID); err != nil {
				logger.Errorf("Failed to requeue the command: %s", err)
				return
			}
		}

		for {
			// Dequeue the next command (this blocks if queue is empty)
			// We need to handle context cancellation while blocked on Dequeue
			commandID, commandPriority, err := s.queue.Dequeue(s.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				logger.Errorf("Failed to load command: %v", err)
				return
			}

			command, err := s.store.LoadCommand(commandID)
			if err != nil {
				logger.Errorf("Failed to load command: %v", err)
				return
			}

			// Set this as the current command
			s.mu.Lock()
			s.currentCmd = &commandID
			s.mu.Unlock()

			task, alreadyRunning, err := s.getNextTask(command)
			if err != nil {
				logger.Errorf("Cannot find the next task: %s", err)
				continue
			}

			// No more tasks, command is complete
			if task == nil {
				command.MarkAsFinished(true)

				if saveErr := s.store.SaveCommand(command); saveErr != nil {
					logger.Errorf("Failed to save command: %v", saveErr)
					return
				}
			} else {
				eventType := api.OnTaskStarted

				if alreadyRunning {
					eventType = api.OnTaskRestarted
				}

				if err := s.invokeHandler(api.Event{
					EventType: eventType,
					Command:   *command,
					Task:      *task,
				}); err != nil {
					if err == context.Canceled {
						return
					}
					// handle error in task
					// 1. Finish the command as failed.
					// 2. Save the command.

					// Mark command as failed
					command.MarkAsFinished(false)

					// Save command
					if saveErr := s.store.SaveCommand(command); saveErr != nil {
						logger.Errorf("Failed to save command: %v", saveErr)
						return
					}
				} else {
					// task finished with success.
					// 1. Set the task as completed.
					// 2. Save the command.
					// 2. Requeue the command

					// Task finished successfully
					task.Status = api.TaskStatusCompleted
					task.FinishedAt = time.Now()
					command.Tasks[task.NodeNV] = task

					// Save the command with updated task
					if saveErr := s.store.SaveCommand(command); saveErr != nil {
						logger.Errorf("Failed to save command: %v", saveErr)
						return
					}

					// Requeue the command to process next task
					if requeueErr := s.queue.Enqueue(s.ctx, commandID, commandPriority); requeueErr != nil {
						logger.Errorf("Failed to requeue command: %v", requeueErr)
						return
					}
				}

			}
			// Clear current command
			s.mu.Lock()
			s.currentCmd = nil
			s.mu.Unlock()
		}
	}()

	return schedulerErrChan
}

// getNextTask returns the next available task to execute for the given command.
func (s *scheduler) getNextTask(command *api.Command) (*api.Task, bool, error) {
	// Get the reverse depth map (depth -> nodes at that depth)
	reverseDepthMap := s.additiveDAG.GetReverseDepthMap()

	// Find max depth
	maxDepth := uint(0)
	for depth := range reverseDepthMap {
		if depth > maxDepth {
			maxDepth = depth
		}
	}

	// Process tasks depth by depth (from 0 to maxDepth)
	for depth := uint(0); depth <= maxDepth; depth++ {
		nodesAtDepth, exists := reverseDepthMap[depth]
		if !exists {
			continue // Skip empty depths
		}

		// Look for next task at current depth
		for _, nv := range nodesAtDepth {
			task, exists := command.Tasks[nv]

			if !exists {
				// Task doesn't exist, create it and return it
				newTask := &api.Task{
					Status:    api.TaskStatusReady,
					NodeNV:    nv,
					CreatedAt: time.Now(),
					Params:    "", // Get this from the node later. ----------------------------------------------------------------------------------------------------
					CommandID: command.ID,
				}
				command.Tasks[nv] = newTask

				if err := s.invokeHandler(api.Event{
					EventType: api.OnTaskCreated,
					Command:   *command,
					Task:      *newTask,
				}); err != nil {
					return nil, false, err
				}
				return newTask, false, nil
			}

			switch task.Status {
			case api.TaskStatusCompleted: // Already done, continue to next task
				continue

			case api.TaskStatusFailed: // Task failed
				return nil, false, fmt.Errorf("task %s failed", nv.String())

			case api.TaskStatusReady: // Found a task that's ready to run
				return task, false, nil

			case api.TaskStatusRunning: // Task is already running meaning it is interrupted.
				return task, true, nil
			}
		}
	}

	// All tasks completed, no more tasks to run
	return nil, false, nil
}

// This would send an event and waits for the response.
func (s *scheduler) invokeHandler(event api.Event) error {
	logger.Debugf("invoke handler is triggered by node: %s", event.Task.NodeNV)
	eventCh, errCh := s.nodes[event.Task.NodeNV].CommChan()

	select {
	case <-s.ctx.Done():
		return context.Canceled
	case eventCh <- event:
		select {
		case <-s.ctx.Done():
			return nil
		case err := <-errCh:
			return err
		}
	}
}

// sendCloseSignals sends exit signal to all of the nodes without waiting for them.
func (s *scheduler) sendCloseSignals(schedulerErrChan chan error) {
	shutdownEvent := api.Event{
		EventType: api.OnSchedulerExit,
	}

	var wg sync.WaitGroup

	for nv, node := range s.nodes {
		// Get the event channel
		eventChan, _ := node.CommChan()
		// Capture variables for goroutine
		nodeName := nv.String()
		ch := eventChan

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Use a timeout to prevent blocking forever
			select {
			case ch <- shutdownEvent:
				logger.Debugf("Sent shutdown signal to node %s", nodeName)
			case <-time.After(5 * time.Second):
				logger.Warnf("Timeout sending shutdown signal to node %s", nodeName)
			}
		}()
	}

	// Wait for all goroutines to finish, then send nil
	go func() {
		wg.Wait()
		schedulerErrChan <- nil

	}()
}
