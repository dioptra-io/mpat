package mpat

import (
	"context"
	"fmt"
	"time"

	"github.com/dioptra-io/ufuk-research/api"
)

// computeDepths calculates the depth of each node using BFS starting from root nodes (nodes with no dependencies).
func (m *mpat) computeDepths() error {
	// Clear existing depths
	m.depths = make(map[uint][]api.NamedVersion)
	m.maxDepth = 0

	// Find all root nodes (nodes with no dependencies)
	rootNodes := m.findRootNodes()

	// If no nodes exist, return error
	if len(m.nodes) == 0 {
		return fmt.Errorf("no nodes found: graph does not contain any nodes")
	}

	// If no root nodes exist but nodes are present, graph is invalid
	if len(rootNodes) == 0 {
		return fmt.Errorf("no root nodes found: graph may contain cycles or all nodes have dependencies")
	}

	// Initialize depth map and in-degree map for all nodes
	nodeDepths := make(map[api.NamedVersion]uint)
	inDegree := make(map[api.NamedVersion]int)

	// Calculate in-degree for each node (number of dependencies)
	for nv := range m.nodes {
		inDegree[nv] = len(m.deps[nv])
	}

	// BFS queue: initialize with root nodes at depth 0. The capacity is set to num nodes as we expect a low number of
	// nodes that are registered.
	queue := make([]api.NamedVersion, 0, len(m.nodes))
	for _, root := range rootNodes {
		nodeDepths[root] = 0
		queue = append(queue, root)
	}

	// Process queue using BFS
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		currentDepth := nodeDepths[current]

		// Update maxDepth
		if currentDepth > m.maxDepth {
			m.maxDepth = currentDepth
		}

		// Process all nodes that depend on current
		for _, dependent := range m.revDeps[current] {
			// Calculate new depth based on this dependency
			newDepth := currentDepth + 1

			// Update depth if this path is longer
			if existingDepth, exists := nodeDepths[dependent]; !exists || newDepth > existingDepth {
				nodeDepths[dependent] = newDepth
			}

			// Decrement in-degree
			inDegree[dependent]--

			// If all dependencies processed, add to queue
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// Verify all nodes have been assigned a depth
	if len(nodeDepths) != len(m.nodes) {
		return fmt.Errorf("failed to assign depths to all nodes: %d/%d processed", len(nodeDepths), len(m.nodes))
	}

	// Populate the depths map
	for nv, depth := range nodeDepths {
		m.depths[depth] = append(m.depths[depth], nv)
	}

	return nil
}

// Returns all nodes that have no dependencies, in-degree is zero (also called root nodes).
func (m *mpat) findRootNodes() []api.NamedVersion {
	rootNodes := make([]api.NamedVersion, 0)
	for nv := range m.nodes {
		if len(m.deps[nv]) == 0 {
			rootNodes = append(rootNodes, nv)
		}
	}
	return rootNodes
}

// Loads all active commands and their incomplete tasks into the active queue. It also marks tasks as orphan if their
// node no longer exists.
func (m *mpat) loadIncompleteCommands() error {
	// Find all active commands
	var commands []api.Command
	if err := m.db.Where("active = ?", true).Find(&commands).Error; err != nil {
		return fmt.Errorf("failed to load active commands: %w", err)
	}

	// Add all active commands to the queue
	for _, cmd := range commands {
		m.activeQueue = append(m.activeQueue, cmd.ID)
	}

	// Load all tasks and mark orphans
	var tasks []api.Task
	if err := m.db.Find(&tasks).Error; err != nil {
		return fmt.Errorf("failed to load tasks: %w", err)
	}

	// Check each task and mark as orphan if node doesn't exist
	for i := range tasks {
		task := &tasks[i]

		// Check if the node still exists
		if _, exists := m.nodes[task.NodeNamedVersion]; !exists {
			// Mark as orphan if not already marked
			if !task.Orphan {
				task.Orphan = true
				if err := m.db.Save(task).Error; err != nil {
					return fmt.Errorf("failed to mark task %d as orphan: %w", task.ID, err)
				}
			}
		}
	}

	return nil
}

// run is the main execution loop that processes tasks
func (m *mpat) run(ctx context.Context) {
	defer close(m.stoppedChan)
	defer func() {
		m.mu.Lock()
		m.started = false
		m.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled
			return
		case <-m.stopChan:
			// Stop requested
			return
		default:
			// Try to execute next task
			if err := m.executeNextTask(ctx); err != nil {
				// Log error but continue (could be no tasks available)
				// If no tasks available, sleep briefly to avoid busy loop
				time.Sleep(100 * time.Millisecond)

				// Other queue functions can signal when the work is available but for now there is a busy loop.
			}
		}
	}
}

// executeNextTask finds and executes the next available task
func (m *mpat) executeNextTask(ctx context.Context) error {
	m.mu.Lock()
	if m.runningTask {
		m.mu.Unlock()
		return fmt.Errorf("task already running")
	}
	m.mu.Unlock()

	// Find the highest priority command
	command, err := m.getHighestPriorityCommand()
	if err != nil {
		return err
	}
	if command == nil {
		// No active commands, return nil (not an error)
		return nil
	}

	// Find the next available task for this command
	task, node, err := m.findNextAvailableTask(command)
	if err != nil {
		return err
	}

	if task == nil {
		// No available tasks - all tasks are done, mark command as complete
		if err := m.completeCommand(command.ID); err != nil {
			return fmt.Errorf("failed to complete command %d: %w", command.ID, err)
		}
		return nil
	}

	// Execute the task
	return m.executeTask(ctx, command, task, node)
}

// completeCommand marks a command as inactive and removes it from the active queue
func (m *mpat) completeCommand(commandID uint) error {
	// Remove from active queue
	m.mu.Lock()
	// Remove from active queue
	newQueue := make([]uint, 0, len(m.activeQueue)-1)
	for _, id := range m.activeQueue {
		if id != commandID {
			newQueue = append(newQueue, id)
		}
	}
	m.activeQueue = newQueue
	m.mu.Unlock()

	// Mark command as inactive
	var command api.Command
	if err := m.db.First(&command, commandID).Error; err != nil {
		return fmt.Errorf("failed to find command: %w", err)
	}

	command.Active = false
	if err := m.db.Save(&command).Error; err != nil {
		return fmt.Errorf("failed to mark command as inactive: %w", err)
	}

	return nil
}

// getHighestPriorityCommand finds the active command with highest priority
func (m *mpat) getHighestPriorityCommand() (*api.Command, error) {
	var commands []api.Command
	if err := m.db.Where("active = ?", true).Order("priority DESC, created_at ASC, id ASC").Find(&commands).Error; err != nil {
		return nil, fmt.Errorf("failed to find active commands: %w", err)
	}

	if len(commands) == 0 {
		// No error, just no commands available
		return nil, nil
	}

	return &commands[0], nil
}

// findNextAvailableTask finds the next task that can be executed
func (m *mpat) findNextAvailableTask(command *api.Command) (*api.Task, Node, error) {
	// Get all tasks for this command
	var tasks []api.Task
	if err := m.db.Where("command_id = ?", command.ID).Find(&tasks).Error; err != nil {
		return nil, nil, fmt.Errorf("failed to load tasks: %w", err)
	}

	// Build a map of task status by node
	taskStatusMap := make(map[api.NamedVersion]api.Status)
	taskMap := make(map[api.NamedVersion]*api.Task)
	for i := range tasks {
		// Skip orphan tasks
		if tasks[i].Orphan {
			continue
		}
		taskStatusMap[tasks[i].NodeNamedVersion] = tasks[i].Status
		taskMap[tasks[i].NodeNamedVersion] = &tasks[i]
	}

	// Find the lowest depth where we can execute a task
	for depth := uint(0); depth <= m.maxDepth; depth++ {
		nodesAtDepth := m.depths[depth]

		// Find an idle task at this depth whose dependencies are satisfied
		for _, nv := range nodesAtDepth {
			status, exists := taskStatusMap[nv]

			// If task doesn't exist for a registered node, that's an error
			if !exists {
				return nil, nil, fmt.Errorf("task for node %v does not exist in command %d", nv, command.ID)
			}

			if status != api.StatusIdle {
				continue
			}

			// Check if this node exists
			node, nodeExists := m.nodes[nv]
			if !nodeExists {
				continue
			}

			// Check if all dependencies of this specific node are done
			dependenciesSatisfied := true
			for _, dep := range m.deps[nv] {
				depStatus, depExists := taskStatusMap[dep]
				if !depExists || depStatus != api.StatusDone {
					dependenciesSatisfied = false
					break
				}
			}

			if dependenciesSatisfied {
				return taskMap[nv], node, nil
			}
		}
	}

	return nil, nil, nil // No available tasks (not an error)
}

// executeTask executes a single task
func (m *mpat) executeTask(parentCtx context.Context, command *api.Command, task *api.Task, node Node) error {
	// Create cancellable context for this task
	taskCtx, cancel := context.WithCancel(parentCtx)

	// Mark task as running
	m.mu.Lock()
	m.runningTask = true
	m.currentCommandID = command.ID
	m.currentTaskNodeNamedVersion = task.NodeNamedVersion
	m.currentTaskCancel = cancel
	m.mu.Unlock()

	// Update task status in database
	task.Status = api.StatusRunning
	if err := m.db.Save(task).Error; err != nil {
		m.mu.Lock()
		m.runningTask = false
		m.currentTaskCancel = nil
		m.mu.Unlock()
		cancel()
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// Execute the task
	err := node.OnTaskRun(taskCtx, command, task)

	// Clean up
	cancel()
	m.mu.Lock()
	m.runningTask = false
	m.currentCommandID = 0
	m.currentTaskNodeNamedVersion = api.NamedVersion{}
	m.currentTaskCancel = nil
	m.mu.Unlock()

	// Update task status based on result
	if err != nil {
		if taskCtx.Err() == context.Canceled {
			// Task was cancelled - don't set finish time
			task.Status = api.StatusIdle
			task.FinishedAt = time.Time{} // Clear finish time
		} else {
			// Task failed - set finish time
			task.Status = api.StatusFailed
			task.FinishedAt = time.Now()
		}
	} else {
		// Task completed successfully
		task.Status = api.StatusDone
		task.FinishedAt = time.Now()
	}

	if err := m.db.Save(task).Error; err != nil {
		return fmt.Errorf("failed to save final task status: %w", err)
	}

	return nil
}
