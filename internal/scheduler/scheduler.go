package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/sirupsen/logrus"
)

// This is the task scheduler. The usage is that the nodes are added and then the dependencies are frozen to perform the
// compuation. The commands can be added to the queue at any time. Each command would spawn tasks t run one by one. Note
// that the queue is designed to run a single task at a  time.
type Scheduler interface {
	// EnqueueCommand inserts a new command into the ready queue. Scheduler sets c.Status = ready.
	EnqueueCommand(c *api.Command) (*api.Command, error)

	// PopReadyCommand removes the next ready command and transitions it to running. Returns ErrNoReadyCommand if none
	// are ready.
	PopReadyCommand(ctx context.Context) (*api.Command, error)

	// PauseCommand moves a running or ready command to sleeping. Returns ErrCommandFinished if the command is
	// completed/failed.
	PauseCommand(commandID uint) error

	// RequeueCommand takes a sleeping command and returns it to ready. Returns ErrCommandNotSleeping or
	// ErrCommandFinished.
	RequeueCommand(commandID uint) (*api.Command, error)

	// Run executes the main scheduling loop (one task at a time). Cancelling ctx stops the loop.
	Run(ctx context.Context) error

	// Returns the command currently executing. If none active, returns ErrNoActiveCommand.
	CurrentCommandID() (uint, error)

	// Provides access to the underlying persistent store.
	Store() Store
}

var _ Scheduler = (*scheduler)(nil)

// Scheduler implementation
type scheduler struct {
	mu               sync.RWMutex
	logger           logrus.FieldLogger
	store            Store
	queue            Queue
	additiveDAG      AdditiveDAG
	currentCommandID *uint // Pointer to allow nil (no current command)
}

// NewScheduler creates a new scheduler with the given store, queue, and nodes. It builds the dependency graph from the
// nodes. Nodes must be provided in topological order (dependencies before dependents).
func NewScheduler(store Store, logger logrus.FieldLogger, nodesToRegister ...Node) (Scheduler, error) {
	if store == nil {
		return nil, fmt.Errorf("store cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	for _, node := range nodesToRegister {
		if node == nil {
			return nil, fmt.Errorf("node cannot be nil")
		}
	}

	logger.Debugln("NewScheduler: health check completed")

	s := &scheduler{
		mu:               sync.RWMutex{},
		logger:           logger,
		store:            store,
		queue:            NewLinkedQueue(),
		additiveDAG:      NewAdditiveDAG(),
		currentCommandID: nil,
	}

	for _, node := range nodesToRegister {
		nv := node.NamedVersion()
		dependencies := node.Dependencies()

		// Add node to the AdditiveDAG, note that this should be in topological order.
		if err := s.additiveDAG.AddNode(node, dependencies...); err != nil {
			return nil, fmt.Errorf("failed to add node %s to DAG: %w", nv.String(), err)
		}
	}

	logger.Debugln("NewScheduler: created additive dag with success")

	return s, nil
}

// EnqueueCommand inserts a new command into the ready queue. Scheduler sets c.Status = ready.
func (s *scheduler) EnqueueCommand(c *api.Command) (*api.Command, error) {
	s.logger.Debug("EnqueueCommand: called")

	if c == nil {
		return nil, fmt.Errorf("command cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Prevent enqueueing finished commands.
	if c.IsFinished() {
		return nil, ErrCommandFinished
	}

	// Scheduler defines lifecycle state.
	c.Status = api.CommandStatusReady

	// Persist the command.
	if err := s.store.CreateCommand(c); err != nil {
		s.logger.WithError(err).Error("EnqueueCommand: failed to create command")
		return nil, err
	}

	// Add command to the ready queue.
	if err := s.queue.Enqueue(c.ID, c.Priority); err != nil {
		s.logger.WithFields(logrus.Fields{
			"commandID": c.ID,
			"priority":  c.Priority,
		}).WithError(err).Error("EnqueueCommand: failed to enqueue command")
		return nil, err
	}

	s.logger.WithFields(logrus.Fields{
		"commandID": c.ID,
		"priority":  c.Priority,
	}).Debug("EnqueueCommand: command enqueued successfully")

	return c, nil
}

// PopReadyCommand removes the next ready command and transitions it to running. Returns ErrNoReadyCommand if none are
// ready.
func (s *scheduler) PopReadyCommand(ctx context.Context) (*api.Command, error) {
	s.logger.Debug("PopReadyCommand: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Attempt to dequeue the next command.
	// Priority is returned but we don't need it here.
	cmdID, _, err := s.queue.Dequeue(ctx)
	if err != nil {
		// Queue may return something like context errors or internal queue errors.
		// If it's empty and blocking behavior is disabled, it should return ErrNoReadyCommand.
		if errors.Is(err, ErrNoReadyCommand) {
			return nil, ErrNoReadyCommand
		}
		s.logger.WithError(err).Error("PopReadyCommand: queue dequeue failed")
		return nil, err
	}

	// Load the command from the store.
	cmd, err := s.store.LoadCommand(cmdID)
	if err != nil {
		s.logger.WithError(err).Error("PopReadyCommand: load command failed")
		return nil, err
	}
	if cmd == nil {
		// The queue may sometimes contain stale IDs (e.g. deletion or corruption).
		s.logger.WithField("commandID", cmdID).Warn("PopReadyCommand: command not found, skipping")
		return nil, ErrCommandNotFound
	}

	// If the command is already finished, it should not be popped.
	if cmd.IsFinished() {
		// Command is invalid for running — drop it silently from queue.
		s.logger.WithField("commandID", cmdID).
			Warn("PopReadyCommand: command is finished, cannot run")
		return nil, ErrCommandFinished
	}

	// The scheduler can pop it only if it's in ready state.
	if cmd.Status != api.CommandStatusReady {
		s.logger.WithFields(logrus.Fields{
			"commandID": cmdID,
			"status":    cmd.Status,
		}).Warn("PopReadyCommand: command not in ready state")
		return nil, ErrCommandNotSleeping // best available error, or define ErrCommandNotReady
	}

	// Transition to running.
	cmd.Status = api.CommandStatusRunning
	if err := s.store.UpdateCommand(cmd); err != nil {
		s.logger.WithError(err).Error("PopReadyCommand: failed to update command to running")
		return nil, err
	}

	// Set as current command.
	s.currentCommandID = &cmd.ID

	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"priority":  cmd.Priority,
	}).Debug("PopReadyCommand: command popped and set to running")

	return cmd, nil
}

// PauseCommand moves a running or ready command to sleeping. Returns ErrCommandFinished if the command is
// completed/failed.
func (s *scheduler) PauseCommand(commandID uint) error {
	s.logger.WithField("commandID", commandID).Debug("PauseCommand: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Load the command.
	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		s.logger.WithError(err).Error("PauseCommand: failed to load command")
		return err
	}
	if cmd == nil {
		return ErrCommandNotFound
	}

	// If the command is completed or failed => cannot pause.
	if cmd.IsFinished() {
		return ErrCommandFinished
	}

	// If already sleeping => error.
	if cmd.Status == api.CommandStatusSleeping {
		return ErrCommandAlreadySleeping
	}

	// If running, we must clear currentCmd.
	if s.currentCommandID != nil && *s.currentCommandID == commandID {
		s.currentCommandID = nil
	}

	// If ready, remove from queue.
	if cmd.Status == api.CommandStatusReady {
		if err := s.queue.Remove(commandID); err != nil {
			s.logger.WithError(err).Warn("PauseCommand: failed to remove command from queue; queue may be stale")
			// We do not return an error here because the DB state should win over the queue
		}
	}

	// Transition to sleeping.
	cmd.Status = api.CommandStatusSleeping

	// Persist.
	if err := s.store.UpdateCommand(cmd); err != nil {
		s.logger.WithError(err).Error("PauseCommand: failed to update command to sleeping")
		return err
	}

	s.logger.WithField("commandID", commandID).Debug("PauseCommand: command paused successfully")
	return nil
}

// RequeueCommand moves a sleeping command back into ready. Returns ErrCommandNotSleeping or ErrCommandFinished.
func (s *scheduler) RequeueCommand(commandID uint) (*api.Command, error) {
	s.logger.WithField("commandID", commandID).Debug("RequeueCommand: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Load command
	cmd, err := s.store.LoadCommand(commandID)
	if err != nil {
		s.logger.WithError(err).Error("RequeueCommand: failed to load command")
		return nil, err
	}
	if cmd == nil {
		return nil, ErrCommandNotFound
	}

	// Finished commands cannot be requeued.
	if cmd.IsFinished() {
		return nil, ErrCommandFinished
	}

	switch cmd.Status {
	case api.CommandStatusSleeping:
		// OK — this is the only valid transition
	case api.CommandStatusReady:
		return nil, ErrCommandNotSleeping
	case api.CommandStatusRunning:
		return nil, ErrCommandNotSleeping
	default:
		// Includes: completed, failed — already handled by IsFinished()
		return nil, ErrCommandNotSleeping
	}

	// Transition to ready.
	cmd.Status = api.CommandStatusReady

	// Persist state change.
	if err := s.store.UpdateCommand(cmd); err != nil {
		s.logger.WithError(err).Error("RequeueCommand: failed updating command to ready")
		return nil, err
	}

	// Enqueue it.
	if err := s.queue.Enqueue(cmd.ID, cmd.Priority); err != nil {
		s.logger.WithError(err).Error("RequeueCommand: failed to enqueue command")
		return nil, err
	}

	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"priority":  cmd.Priority,
	}).Debug("RequeueCommand: command moved to ready queue")

	return cmd, nil
}

// Run executes the main scheduling loop (one task at a time). Cancelling ctx stops the loop.
func (s *scheduler) Run(ctx context.Context) error {
	s.logger.Debug("Run: scheduler loop starting")

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Run: context cancelled, stopping scheduler loop")
			return ctx.Err()
		default:
			// Continue loop
		}

		// Step 1: Pull next command from the queue (blocking until ctx is cancelled)
		cmdID, priority, err := s.queue.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.logger.Debug("Run: dequeue cancelled via context")
				return err
			}
			if errors.Is(err, ErrNoReadyCommand) {
				// Shouldn’t happen because dequeue blocks, but we’ll treat it as a soft continue
				s.logger.Debug("Run: no ready commands")
				continue
			}

			s.logger.WithError(err).Error("Run: unexpected dequeue error")
			return err
		}

		// Step 2: Load command
		cmd, err := s.store.LoadCommand(cmdID)
		if err != nil {
			s.logger.WithError(err).Error("Run: failed to load command")
			return err
		}
		if cmd == nil {
			s.logger.WithField("commandID", cmdID).Warn("Run: stale command ID from queue")
			continue // if the command no longer exists we continue
		}

		// Step 3: Validate command state
		switch cmd.Status {

		case api.CommandStatusReady, api.CommandStatusRunning:
			// Good — this is what we expect

		default:
			// Sleeping, finished, failed → shouldn't be in queue
			s.logger.WithFields(logrus.Fields{
				"commandID": cmd.ID,
				"status":    cmd.Status,
			}).Warn("Run: command state invalid for execution")
			continue
		}

		// Step 3.1 set the command status as running.
		cmd.Status = api.CommandStatusRunning
		if err := s.store.UpdateCommand(cmd); err != nil {
			return err
		}

		// Step 4: Get the next task for this command
		task, err := s.nextAvailableTask(cmd)
		if err != nil {
			if errors.Is(err, ErrNoAvailableTask) {
				// No tasks left → command is finished
				s.logger.WithField("commandID", cmd.ID).
					Debug("Run: command has no more tasks")
				s.markCommandFinished(cmd)
				continue
			} else if errors.Is(err, ErrTaskFailed) {
				s.logger.WithField("commandID", cmd.ID).
					Debug("Run: task under a comand has failed")
				s.markTaskFailed(cmd, task, err)
				continue
			} else if errors.Is(err, ErrTaskAlreadyRunning) {
				// Task was already running, execute task function will handle it.
			} else {
				s.logger.WithError(err).Error("Run: failed to get next available task")
				continue
			}
		}

		// Step 5: Execute task, can be in the ready or running state.
		if err := s.executeTask(ctx, cmd, task); err != nil {
			s.logger.WithError(err).Error("Run: task execution failed")
			s.markTaskFailed(cmd, task, err)
			continue
		}

		// Task succeeded:
		if err := s.markTaskCompleted(cmd, task); err != nil {
			return err
		}

		// Reenqueue the commandID
		if err := s.queue.Enqueue(cmdID, priority); err != nil {
			s.logger.WithError(err).Error("Run: unexpected enqueue error")
			return err
		}
	}
}

// CurrentCommandID returns the currently executing command ID. If none active, returns ErrNoActiveCommand.
func (s *scheduler) CurrentCommandID() (uint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.currentCommandID == nil {
		return 0, ErrNoActiveCommand
	}

	return *s.currentCommandID, nil
}

// Store returns the underlying persistent store.
func (s *scheduler) Store() Store {
	return s.store
}

// Private Methods

// This would send an event and waits for the response. This deadlocks if the nodes handler implementation deadlocks.
func (s *scheduler) invokeHandler(ctx context.Context, event api.Event) error {
	node, err := s.additiveDAG.GetNode(event.Task.NodeNV)
	if err != nil {
		return err
	}

	eventCh, errCh := node.CommChan()

	select {
	case <-ctx.Done():
		return context.Canceled
	case eventCh <- event:
		select {
		case <-ctx.Done():
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

	for _, node := range s.additiveDAG.GetNodes() {
		eventChan, _ := node.CommChan()
		nv := node.NamedVersion()
		nodeName := nv.String()
		ch := eventChan

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Use a timeout to prevent blocking forever
			select {
			case ch <- shutdownEvent:
				s.logger.Debugf("Sent shutdown signal to node %s", nodeName)
			case <-time.After(5 * time.Second):
				s.logger.Warnf("Timeout sending shutdown signal to node %s", nodeName)
			}
		}()
	}

	// Wait for all goroutines to finish, then send nil
	go func() {
		wg.Wait()
		schedulerErrChan <- nil
	}()
}

// nextAvailableTask returns the next runnable task for a given command. If no tasks remain, returns ErrNoAvailableTask.
func (s *scheduler) nextAvailableTask(cmd *api.Command) (*api.Task, error) {
	s.logger.WithField("commandID", cmd.ID).Debug("nextAvailableTask: called")

	// 1. Get reverse depth map (depth → []nodes)
	rev := s.additiveDAG.GetReverseDepthMap()

	// 2. Compute max depth
	var maxDepth uint
	for d := range rev {
		if d > maxDepth {
			maxDepth = d
		}
	}

	// 3. Iterate depths in strict order 0 → maxDepth
	for depth := uint(0); depth <= maxDepth; depth++ {
		nodesAtDepth := rev[depth]

		// Iterate nodes in insertion order
		for _, nv := range nodesAtDepth {

			// Step A: Try loading the task for this node.
			task, err := s.store.LoadTask(cmd.ID, nv)
			if err != nil {
				s.logger.WithError(err).Error("nextAvailableTask: failed to load task")
				return nil, err
			}

			// Step B: If task does not exist → create it
			if task == nil {
				s.logger.WithFields(logrus.Fields{
					"commandID": cmd.ID,
					"node_nv":   nv.String(),
				}).Debug("nextAvailableTask: task does not exist, creating")

				newTask := &api.Task{
					CommandID: cmd.ID,
					NodeNV:    nv,
					Status:    api.TaskStatusReady,
					Params:    "",
				}

				if err := s.store.CreateTask(newTask); err != nil {
					s.logger.WithError(err).Error("nextAvailableTask: failed to create new task")
					return nil, err
				}

				// Newly created tasks are ready → return immediately
				return newTask, nil
			}

			// Step C: Evaluate task status
			switch task.Status {

			case api.TaskStatusReady:
				// Return runnable task
				return task, nil

			case api.TaskStatusRunning:
				// Command was interrupted → scheduler must handle
				s.logger.WithField("node_nv", nv.String()).
					Debug("nextAvailableTask: task already running")
				return task, ErrTaskAlreadyRunning

			case api.TaskStatusCompleted, api.TaskStatusOrphaned:
				// Skip this node and move on
				continue

			case api.TaskStatusFailed:
				s.logger.WithFields(logrus.Fields{
					"commandID": cmd.ID,
					"node_nv":   task.NodeNV.String(),
				}).Error("nextAvailableTask: task is failed")
				return nil, ErrTaskFailed
			}
		}
	}

	// 3. No runnable tasks found
	return nil, ErrNoAvailableTask
}

// executeTask runs the handler for the given task and blocks until completion.
func (s *scheduler) executeTask(ctx context.Context, cmd *api.Command, task *api.Task) error {
	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"node_nv":   task.NodeNV.String(),
		"status":    task.Status,
	}).Debug("executeTask: called")

	s.mu.Lock()

	// Detect restart vs fresh start
	var eventType api.HandlerType
	if task.Status == api.TaskStatusRunning {
		eventType = api.OnTaskRestarted
	} else {
		eventType = api.OnTaskStarted

		// Fresh task → set to running
		task.Status = api.TaskStatusRunning
		// UpdatedAt is updated by GORM automatically
		if err := s.store.UpdateTask(task); err != nil {
			s.mu.Unlock()
			s.logger.WithError(err).Error("executeTask: failed to update task to running")
			return err
		}
	}

	// Set current command
	s.currentCommandID = &cmd.ID

	s.mu.Unlock()

	// Build the event (copy command & task by value)
	event := api.Event{
		EventType: eventType,
		Command:   cmd,
		Task:      task,
	}

	// Invoke the handler — may block for a LONG time
	if err := s.invokeHandler(ctx, event); err != nil {
		s.logger.WithError(err).Error("executeTask: handler returned error")
		return err
	}

	return nil
}

// markTaskCompleted updates the task to completed and persists the change.
func (s *scheduler) markTaskCompleted(cmd *api.Command, task *api.Task) error {
	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"node_nv":   task.NodeNV.String(),
	}).Debug("markTaskCompleted: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Update the task fields
	now := time.Now()
	task.FinishedAt = &now
	task.Status = api.TaskStatusCompleted

	if err := s.store.UpdateTask(task); err != nil {
		s.logger.WithError(err).Error("markTaskCompleted: failed to update task")
		return err
	}

	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"node_nv":   task.NodeNV.String(),
	}).Debug("markTaskCompleted: task updated to completed")

	return nil
}

// markTaskFailed updates a task to failed and propagates failure to command.
func (s *scheduler) markTaskFailed(cmd *api.Command, task *api.Task, taskErr error) error {
	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"node_nv":   task.NodeNV.String(),
		"error":     taskErr,
	}).Error("markTaskFailed: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// --- 1. Mark the task itself as failed ---
	now := time.Now()
	task.FinishedAt = &now
	task.Status = api.TaskStatusFailed

	if err := s.store.UpdateTask(task); err != nil {
		s.logger.WithError(err).Error("markTaskFailed: failed to update task state")
		return err
	}

	// --- 2. Immediately mark the command as failed ---
	cmd.Status = api.CommandStatusFailed
	cmd.FinishedAt = &now

	if err := s.store.UpdateCommand(cmd); err != nil {
		s.logger.WithError(err).Error("markTaskFailed: failed to update command state")
		return err
	}

	// --- 3. Clear current command if needed ---
	if s.currentCommandID != nil && *s.currentCommandID == cmd.ID {
		s.currentCommandID = nil
	}

	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"node_nv":   task.NodeNV.String(),
	}).Error("markTaskFailed: command marked failed due to task failure")

	return nil
}

// markCommandFinished marks the command as completed (if success) or failed.
func (s *scheduler) markCommandFinished(cmd *api.Command) error {
	s.logger.WithField("commandID", cmd.ID).Debug("markCommandFinished: called")

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Reload the command to ensure we have all its tasks
	updatedCmd, err := s.store.LoadCommand(cmd.ID)
	if err != nil {
		s.logger.WithError(err).Error("markCommandFinished: failed to reload command")
		return err
	}
	if updatedCmd == nil {
		return ErrCommandNotFound
	}

	// 2. Scan all tasks
	anyFailed := false
	allDone := true

	for _, t := range updatedCmd.Tasks {
		switch t.Status {

		case api.TaskStatusFailed:
			anyFailed = true

		case api.TaskStatusCompleted, api.TaskStatusOrphaned:
			// finished states, okay

		default:
			// Not ready/running here because nextAvailableTask would have returned it
			allDone = false
		}
	}

	now := time.Now()

	// 3. Decide command state
	if anyFailed {
		updatedCmd.Status = api.CommandStatusFailed
		updatedCmd.FinishedAt = &now
	} else if allDone {
		updatedCmd.Status = api.CommandStatusCompleted
		updatedCmd.FinishedAt = &now
	} else {
		// This should never happen — it would indicate scheduler inconsistency
		s.logger.WithField("commandID", cmd.ID).
			Error("markCommandFinished: unexpected: command not fully done but no runnable tasks")
		return fmt.Errorf("command %d in inconsistent state", cmd.ID)
	}

	// 4. Persist command update
	if err := s.store.UpdateCommand(updatedCmd); err != nil {
		s.logger.WithError(err).Error("markCommandFinished: failed to update command state")
		return err
	}

	// 5. Clear active command ID
	if s.currentCommandID != nil && *s.currentCommandID == cmd.ID {
		s.currentCommandID = nil
	}

	s.logger.WithFields(logrus.Fields{
		"commandID": cmd.ID,
		"status":    updatedCmd.Status,
	}).Debug("markCommandFinished: command state updated and currentCmd cleared")

	return nil
}

// func NewScheduler(store Store, nodesToRegister ...Node) (Scheduler, error) {
// 	if store == nil {
// 		return nil, fmt.Errorf("store cannot be nil")
// 	}
//
// 	s := &scheduler{
// 		store:       store,
// 		queue:       NewLinkedQueue(),
// 		additiveDAG: NewAdditiveDAG(),
// 	}
//
// 	// Add all nodes to the map and DSG
// 	for _, node := range nodes {
// 		nv := node.NamedVersion()
// 		deps := node.Dependencies()
//
// 		for _, depNV := range deps {
// 			if _, ok := s.nodes[depNV]; !ok {
// 				return nil, fmt.Errorf("node %s depends on %s which has not been added yet (nodes must be in topological order)", nv.String(), depNV.String())
// 			}
// 		}
// 		s.nodes[nv] = node
//
// 		// Add node to DSG
// 		if err := s.additiveDAG.AddNode(node, deps...); err != nil {
// 			return nil, fmt.Errorf("failed to add node %s to DSG: %w", nv.String(), err)
// 		}
// 	}
//
// 	return s, nil
// }

// func (s *scheduler) GetCurrentCommandID() (*uint, error) {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
//
// 	return s.currentCmd, nil
// }
//
// func (s *scheduler) GetCommand(commandID uint) (*api.Command, error) {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
//
// 	cmd, err := s.store.LoadCommand(commandID)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return cmd, nil
// }
//
// func (s *scheduler) SetPriority(commandID uint, p uint) error {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
//
// 	// Get the command
// 	cmd, err := s.store.LoadCommand(commandID)
// 	if err != nil {
// 		return err
// 	}
//
// 	// Update priority
// 	cmd.Priority = p
// 	if err := s.store.SaveCommand(cmd); err != nil {
// 		return fmt.Errorf("failed to update command priority: %w", err)
// 	}
//
// 	return nil
// }
//
// func (s *scheduler) ListCommands() ([]*api.Command, error) {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
//
// 	commandIDs, err := s.store.GetAllCommandIDs()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to load commands: %w", err)
// 	}
//
// 	commands := make([]*api.Command, 0, len(commandIDs))
// 	for _, commandID := range commandIDs {
// 		command, err := s.store.LoadCommand(commandID)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to load command: %w", err)
// 		}
//
// 		commands = append(commands, command)
// 	}
//
// 	return commands, nil
// }
//
// func (s *scheduler) ListTasksForCommand(commandID uint) ([]*api.Task, error) {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
//
// 	command, err := s.store.LoadCommand(commandID)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to find tasks for command %d: %w", commandID, err)
// 	}
//
// 	tasks := make([]*api.Task, 0, len(command.Tasks))
// 	for _, task := range command.Tasks {
// 		tasks = append(tasks, task)
// 	}
//
// 	return tasks, nil
// }
//
// func (s *scheduler) EnqueueCommand(params string, p uint) (*api.Command, error) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
//
// 	// Create a new command in the store
// 	cmd, err := s.store.CreateCommand(params)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create command: %w", err)
// 	}
//
// 	// Set priority and status
// 	cmd.Priority = p
// 	cmd.Status = api.CommandStatusExecuting
//
// 	// Save the updated command
// 	if err := s.store.SaveCommand(cmd); err != nil {
// 		return nil, fmt.Errorf("failed to save command: %w", err)
// 	}
//
// 	// Add to queue with priority
// 	if err := s.queue.Enqueue(s.ctx, cmd.ID, p); err != nil {
// 		return nil, fmt.Errorf("failed to enqueue command: %w", err)
// 	}
//
// 	return cmd, nil
// }
//
// func (s *scheduler) DequeueCommand(commandID uint) error {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
//
// 	// Load the command
// 	cmd, err := s.store.LoadCommand(commandID)
// 	if err != nil {
// 		return fmt.Errorf("failed to load command: %w", err)
// 	}
//
// 	// Check if command is finished
// 	if cmd.IsFinished() {
// 		return fmt.Errorf("cannot deactivate finished command %d", commandID)
// 	}
//
// 	// Set status to inactive
// 	cmd.Status = api.CommandStatusSleeping
//
// 	// Save the updated command
// 	if err := s.store.SaveCommand(cmd); err != nil {
// 		return fmt.Errorf("failed to save command: %w", err)
// 	}
//
// 	// Remove the commandID from queue.
// 	if err := s.queue.Remove(s.ctx, commandID); err != nil {
// 		return fmt.Errorf("failed to enqueue command: %w", err)
// 	}
//
// 	return nil
// }
//
// func (s *scheduler) RequeueCommand(commandID uint) (*api.Command, error) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
//
// 	// Load the command
// 	cmd, err := s.store.LoadCommand(commandID)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to load command: %w", err)
// 	}
//
// 	// Check if command is finished
// 	if cmd.IsFinished() {
// 		return nil, fmt.Errorf("cannot requeue finished command %d", commandID)
// 	}
//
// 	// Set status to active
// 	cmd.Status = api.CommandStatusExecuting
//
// 	// Save the updated command
// 	if err := s.store.SaveCommand(cmd); err != nil {
// 		return nil, fmt.Errorf("failed to save command: %w", err)
// 	}
//
// 	// Add to queue with its priority
// 	if err := s.queue.Enqueue(s.ctx, cmd.ID, cmd.Priority); err != nil {
// 		return nil, fmt.Errorf("failed to enqueue command: %w", err)
// 	}
//
// 	return cmd, nil
// }
//
// func (s *scheduler) Start(ctx context.Context) chan error {
// 	schedulerErrChan := make(chan error, 1)
// 	//  the execution loop in a goroutine
// 	go func() {
// 		// Save the context.
// 		s.ctx = ctx
//
// 		// Load the existing commandIDs first and add them to the queue.
// 		defer s.sendCloseSignals(schedulerErrChan)
//
// 		commandIDs, err := s.store.GetAllCommandIDs()
// 		if err != nil {
// 			logger.Errorf("Failed to load the command: %s", err)
// 			return
// 		}
//
// 		for _, commandID := range commandIDs {
// 			if _, err := s.RequeueCommand(commandID); err != nil {
// 				logger.Errorf("Failed to requeue the command: %s", err)
// 				return
// 			}
// 		}
//
// 		for {
// 			// Dequeue the next command (this blocks if queue is empty)
// 			// We need to handle context cancellation while blocked on Dequeue
// 			commandID, commandPriority, err := s.queue.Dequeue(s.ctx)
// 			if err != nil {
// 				if err == context.Canceled {
// 					return
// 				}
// 				logger.Errorf("Failed to load command: %v", err)
// 				return
// 			}
//
// 			command, err := s.store.LoadCommand(commandID)
// 			if err != nil {
// 				logger.Errorf("Failed to load command: %v", err)
// 				return
// 			}
//
// 			// Set this as the current command
// 			s.mu.Lock()
// 			s.currentCmd = &commandID
// 			s.mu.Unlock()
//
// 			task, alreadyRunning, err := s.getNextTask(command)
// 			if err != nil {
// 				logger.Errorf("Cannot find the next task: %s", err)
// 				continue
// 			}
//
// 			// No more tasks, command is complete
// 			if task == nil {
// 				command.MarkAsFinished(true)
//
// 				if saveErr := s.store.SaveCommand(command); saveErr != nil {
// 					logger.Errorf("Failed to save command: %v", saveErr)
// 					return
// 				}
// 			} else {
// 				eventType := api.OnTaskStarted
//
// 				if alreadyRunning {
// 					eventType = api.OnTaskRestarted
// 				}
//
// 				if err := s.invokeHandler(api.Event{
// 					EventType: eventType,
// 					Command:   *command,
// 					Task:      *task,
// 				}); err != nil {
// 					if err == context.Canceled {
// 						return
// 					}
// 					// handle error in task
// 					// 1. Finish the command as failed.
// 					// 2. Save the command.
//
// 					// Mark command as failed
// 					command.MarkAsFinished(false)
//
// 					// Save command
// 					if saveErr := s.store.SaveCommand(command); saveErr != nil {
// 						logger.Errorf("Failed to save command: %v", saveErr)
// 						return
// 					}
// 				} else {
// 					// task finished with success.
// 					// 1. Set the task as completed.
// 					// 2. Save the command.
// 					// 2. Requeue the command
//
// 					// Task finished successfully
// 					task.Status = api.TaskStatusCompleted
// 					task.FinishedAt = time.Now()
// 					command.Tasks[task.NodeNV] = task
//
// 					// Save the command with updated task
// 					if saveErr := s.store.SaveCommand(command); saveErr != nil {
// 						logger.Errorf("Failed to save command: %v", saveErr)
// 						return
// 					}
//
// 					// Requeue the command to process next task
// 					if requeueErr := s.queue.Enqueue(s.ctx, commandID, commandPriority); requeueErr != nil {
// 						logger.Errorf("Failed to requeue command: %v", requeueErr)
// 						return
// 					}
// 				}
//
// 			}
// 			// Clear current command
// 			s.mu.Lock()
// 			s.currentCmd = nil
// 			s.mu.Unlock()
// 		}
// 	}()
//
// 	return schedulerErrChan
// }
//
// // getNextTask returns the next available task to execute for the given command.
// func (s *scheduler) getNextTask(command *api.Command) (*api.Task, bool, error) {
// 	// Get the reverse depth map (depth -> nodes at that depth)
// 	reverseDepthMap := s.additiveDAG.GetReverseDepthMap()
//
// 	// Find max depth
// 	maxDepth := uint(0)
// 	for depth := range reverseDepthMap {
// 		if depth > maxDepth {
// 			maxDepth = depth
// 		}
// 	}
//
// 	// Process tasks depth by depth (from 0 to maxDepth)
// 	for depth := uint(0); depth <= maxDepth; depth++ {
// 		nodesAtDepth, exists := reverseDepthMap[depth]
// 		if !exists {
// 			continue // Skip empty depths
// 		}
//
// 		// Look for next task at current depth
// 		for _, nv := range nodesAtDepth {
// 			task, exists := command.Tasks[nv]
//
// 			if !exists {
// 				// Task doesn't exist, create it and return it
// 				newTask := &api.Task{
// 					Status:    api.TaskStatusReady,
// 					NodeNV:    nv,
// 					CreatedAt: time.Now(),
// 					Params:    "", // Get this from the node later. ----------------------------------------------------------------------------------------------------
// 					CommandID: command.ID,
// 				}
// 				command.Tasks[nv] = newTask
//
// 				if err := s.invokeHandler(api.Event{
// 					EventType: api.OnTaskCreated,
// 					Command:   *command,
// 					Task:      *newTask,
// 				}); err != nil {
// 					return nil, false, err
// 				}
// 				return newTask, false, nil
// 			}
//
// 			switch task.Status {
// 			case api.TaskStatusCompleted: // Already done, continue to next task
// 				continue
//
// 			case api.TaskStatusFailed: // Task failed
// 				return nil, false, fmt.Errorf("task %s failed", nv.String())
//
// 			case api.TaskStatusReady: // Found a task that's ready to run
// 				return task, false, nil
//
// 			case api.TaskStatusRunning: // Task is already running meaning it is interrupted.
// 				return task, true, nil
// 			}
// 		}
// 	}
//
// 	// All tasks completed, no more tasks to run
// 	return nil, false, nil
// }
//
// // This would send an event and waits for the response.
// func (s *scheduler) invokeHandler(event api.Event) error {
// 	logger.Debugf("invoke handler is triggered by node: %s", event.Task.NodeNV)
// 	eventCh, errCh := s.nodes[event.Task.NodeNV].CommChan()
//
// 	select {
// 	case <-s.ctx.Done():
// 		return context.Canceled
// 	case eventCh <- event:
// 		select {
// 		case <-s.ctx.Done():
// 			return nil
// 		case err := <-errCh:
// 			return err
// 		}
// 	}
// }
//
