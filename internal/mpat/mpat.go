package mpat

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/dioptra-io/ufuk-research/api"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Node is also known as a processing Node. It defines a name and some operations. It is a generalization. In MPAT it is
// used to ingress data from different sources, or to run chunked ClickHouse queries to generate database tables.
type Node interface {
	// Returns the named version of the node.
	NamedVersion() api.NamedVersion

	// Gets the default parameters in JSON string for a task.
	GetDefaultTaskParams(c *api.Command) string

	// This is invoked when the task is being run. There are also some other handlers.
	OnTaskRun(ctx context.Context, c *api.Command, t *api.Task) error

	// This is invoked when a task is loaded during requeue with its previous state.
	OnTaskLoad(ctx context.Context, c *api.Command, t *api.Task, prevState api.Status) error

	// This is invoked when a task is created during requeue or enqueue.
	OnTaskCreate(ctx context.Context, c *api.Command, t *api.Task, enqueue bool) error

	// This is invoked when a command is dequeued and the task was idle (stopped).
	OnTaskStopped(ctx context.Context, c *api.Command, t *api.Task) error

	// This is invoked when a command is dequeued and the task was running (interrupted).
	OnTaskInterrupted(ctx context.Context, c *api.Command, t *api.Task) error
}

// This is the task queue. The usage is that the nodes are added and then the dependencies are frozen to perform the
// compuation. The commands can be added to the queue at any time. Each command would spawn tasks t run one by one. Note
// that the queue is designed to run a single task at a  time.
type MPAT interface {
	// Adds a new processing node to the queue with the specified dependencies. It returns an error if the node with
	// that named version already exists.
	RegisterNode(n Node, deps ...api.NamedVersion) error

	// This freezes the node addition and optimizes the created graph. This can only be done once in the lifetime of the
	// struct. If called twice then returns an error.
	FreezeDeps(load bool) error

	// Returns if the dependencies are frozen.
	AreDepsFrozen() bool

	// Adds a command to the queue with the given params and the priority.
	EnqueueCommand(params string, p uint) (*api.Command, error)

	// Removes the command from the queue, stops all its running tasks, marks them as waiting, marks
	// the command as not active.
	DequeueCommand(commandID uint) error

	// Adds the command to the queue again, marks its tasks are idle, marks the command as active.
	RequeueCommand(commandID uint) (*api.Command, error)

	// Starts the execution loop in a separate go routine.
	Start() error

	// Gracefully stops the queue.
	Stop() error

	// Gets the current active command ID, if there are none returns an error.
	GetCurrentCommandID() (uint, error)

	// Gets the command with the given ID, if it doesn't exists then returns an error.
	GetCommand(commandID uint) (*api.Command, error)

	// Sets the priority of a command, this would not stop the if a task of this command is running.
	SetPriority(commandID uint, p uint) error
}

// Creates an instance of MPAT object which uses an sqlite database. The path can also be in memory.
func NewMPAT(path string) (MPAT, error) {
	// Open SQLite database connection
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Run auto-migrations for the required tables
	if err := db.AutoMigrate(&api.Command{}, &api.Task{}); err != nil {
		return nil, fmt.Errorf("failed to auto-migrate database: %w", err)
	}

	// Initialize the mpat struct
	m := &mpat{
		activeQueue:                 make([]uint, 0),
		nodes:                       make(map[api.NamedVersion]Node),
		deps:                        make(map[api.NamedVersion][]api.NamedVersion),
		revDeps:                     make(map[api.NamedVersion][]api.NamedVersion),
		frozen:                      false,
		depths:                      make(map[uint][]api.NamedVersion),
		maxDepth:                    0,
		db:                          db,
		currentTaskCancel:           nil,
		currentCommandID:            0,
		currentTaskNodeNamedVersion: api.NamedVersion{},
		runningTask:                 false,
	}

	return m, nil
}

// The implementation of the MPAT interface.
type mpat struct {
	// This is the active queue of the commands. Each element here is a command ID.
	activeQueue []uint

	// Represents the set of added nodes.
	nodes map[api.NamedVersion]Node

	// Represents the dependencies and reversed dependencies.
	deps    map[api.NamedVersion][]api.NamedVersion
	revDeps map[api.NamedVersion][]api.NamedVersion

	frozen bool

	// Maps depth to a set of Nodes.
	depths   map[uint][]api.NamedVersion
	maxDepth uint

	// sqlite db.
	db *gorm.DB

	// Current running task's cancel function
	currentTaskCancel context.CancelFunc

	// Current running command ID
	currentCommandID uint

	// Current running task's node named version
	currentTaskNodeNamedVersion api.NamedVersion

	// Whether a task is currently running
	runningTask bool

	// Mutex for thread-safe access
	mu sync.RWMutex
}

// Adds a new processing node to the queue with the specified dependencies. It returns an error if the node with that
// named version already exists.
func (m *mpat) RegisterNode(n Node, deps ...api.NamedVersion) error {
	// Check if dependencies are frozen
	if m.frozen {
		return fmt.Errorf("cannot register node: dependencies are frozen")
	}

	// Get the node's named version
	nv := n.NamedVersion()

	// Check if node already exists
	if _, exists := m.nodes[nv]; exists {
		return fmt.Errorf("node with named version %v already exists", nv)
	}

	// Remove duplicate dependencies
	uniqueDeps := make([]api.NamedVersion, 0, len(deps))
	seenDeps := make(map[api.NamedVersion]bool)
	for _, dep := range deps {
		if !seenDeps[dep] {
			seenDeps[dep] = true
			uniqueDeps = append(uniqueDeps, dep)
		}
	}

	// Validate that all dependencies exist
	for _, dep := range uniqueDeps {
		if _, exists := m.nodes[dep]; !exists {
			return fmt.Errorf("dependency %v does not exist", dep)
		}
	}

	// Register the node
	m.nodes[nv] = n

	// Store dependencies
	if len(uniqueDeps) > 0 {
		m.deps[nv] = uniqueDeps
	} else {
		m.deps[nv] = make([]api.NamedVersion, 0)
	}

	// Update reverse dependencies
	for _, dep := range uniqueDeps {
		if m.revDeps[dep] == nil {
			m.revDeps[dep] = make([]api.NamedVersion, 0, 1)
		}
		m.revDeps[dep] = append(m.revDeps[dep], nv)
	}

	// Initialize reverse dependencies for this node if not present
	if m.revDeps[nv] == nil {
		m.revDeps[nv] = make([]api.NamedVersion, 0)
	}

	return nil
}

// FreezeDeps freezes the node addition and optimizes the created graph.
func (m *mpat) FreezeDeps(load bool) error {
	// Check if already frozen
	if m.frozen {
		return fmt.Errorf("dependencies are already frozen")
	}

	// Compute depths using BFS starting from leaf nodes
	if err := m.computeDepths(); err != nil {
		return fmt.Errorf("failed to compute depths: %w", err)
	}

	// Load incomplete commands and tasks if requested
	if load {
		if err := m.loadIncompleteCommands(); err != nil {
			return fmt.Errorf("failed to load incomplete commands: %w", err)
		}
	}

	// Mark as frozen
	m.frozen = true

	return nil
}

// AreDepsFrozen returns if the dependencies are frozen.
func (m *mpat) AreDepsFrozen() bool {
	return m.frozen
}

// EnqueueCommand adds a command to the queue with the given params and the priority.
func (m *mpat) EnqueueCommand(params string, priority uint) (*api.Command, error) {
	// Check if dependencies are frozen
	if !m.frozen {
		return nil, fmt.Errorf("cannot enqueue command: dependencies must be frozen first")
	}

	// Create a new command
	command := &api.Command{
		Params:   params,
		Priority: priority,
		Active:   false, // Set to false initially, RequeueCommand will set it to true
	}

	if err := m.db.Create(command).Error; err != nil {
		return nil, fmt.Errorf("failed to create command in database: %w", err)
	}

	// Requeue the command to create tasks and add to active queue
	return m.RequeueCommand(command.ID)
}

// DequeueCommand removes the command from the queue, stops all its running tasks.
func (m *mpat) DequeueCommand(commandID uint) error {
	// Check if dependencies are frozen
	if !m.frozen {
		return fmt.Errorf("cannot dequeue command: dependencies must be frozen first")
	}

	// Check if command is in the active queue
	if !slices.Contains(m.activeQueue, commandID) {
		return fmt.Errorf("command %d is not in the active queue", commandID)
	}

	// Get the command from database
	var command api.Command
	if err := m.db.First(&command, commandID).Error; err != nil {
		return fmt.Errorf("failed to find command %d: %w", commandID, err)
	}

	m.mu.Lock()
	// If this command has a currently running task, cancel it
	if m.runningTask && m.currentCommandID == commandID {
		if m.currentTaskCancel != nil {
			m.currentTaskCancel()
			m.currentTaskCancel = nil
		}
		m.runningTask = false
		m.currentCommandID = 0
		m.currentTaskNodeNamedVersion = api.NamedVersion{}
	}
	m.mu.Unlock()

	// Remove command from active queue
	newQueue := make([]uint, 0, len(m.activeQueue)-1)
	for _, id := range m.activeQueue {
		if id != commandID {
			newQueue = append(newQueue, id)
		}
	}
	m.activeQueue = newQueue

	// Mark command as inactive
	command.Active = false
	if err := m.db.Save(&command).Error; err != nil {
		return fmt.Errorf("failed to mark command %d as inactive: %w", commandID, err)
	}

	// Get all tasks for this command
	var tasks []api.Task
	if err := m.db.Where("command_id = ?", commandID).Find(&tasks).Error; err != nil {
		return fmt.Errorf("failed to load tasks for command %d: %w", commandID, err)
	}

	// Create context for handlers
	ctx := context.Background()

	// Process each task
	for i := range tasks {
		task := &tasks[i]

		// Skip orphan tasks
		if task.Orphan {
			continue
		}

		// Get the node for this task
		node, exists := m.nodes[task.NodeNamedVersion]
		if !exists {
			continue
		}

		// Handle based on task status
		switch task.Status {
		case api.StatusRunning:
			// Task was running - mark as idle and call interrupted handler
			task.Status = api.StatusIdle
			if err := m.db.Save(task).Error; err != nil {
				return fmt.Errorf("failed to update task %d: %w", task.ID, err)
			}

			if err := node.OnTaskInterrupted(ctx, &command, task); err != nil {
				return fmt.Errorf("failed to call OnTaskInterrupted for task %d: %w", task.ID, err)
			}
		case api.StatusIdle:
			// Task was idle - call stopped handler
			if err := node.OnTaskStopped(ctx, &command, task); err != nil {
				return fmt.Errorf("failed to call OnTaskStopped for task %d: %w", task.ID, err)
			}
		}
		// For other statuses (waiting, failed, done), do nothing
	}

	return nil
}

// RequeueCommand adds the command to the queue again, marks its tasks as idle, marks the command as active.
func (m *mpat) RequeueCommand(commandID uint) (*api.Command, error) {
	// Check if dependencies are frozen
	if !m.frozen {
		return nil, fmt.Errorf("cannot requeue command: dependencies must be frozen first")
	}

	// Get the command from database
	var command api.Command
	if err := m.db.First(&command, commandID).Error; err != nil {
		return nil, fmt.Errorf("failed to find command %d: %w", commandID, err)
	}

	// Check if command is already in the active queue
	if slices.Contains(m.activeQueue, commandID) {
		return nil, fmt.Errorf("command %d is already in the active queue", commandID)
	}

	// Mark command as active
	command.Active = true
	if err := m.db.Save(&command).Error; err != nil {
		return nil, fmt.Errorf("failed to mark command %d as active: %w", commandID, err)
	}

	// Add to active queue
	m.activeQueue = append(m.activeQueue, commandID)

	// Get all tasks for this command
	var tasks []api.Task
	if err := m.db.Where("command_id = ?", commandID).Find(&tasks).Error; err != nil {
		return nil, fmt.Errorf("failed to load tasks for command %d: %w", commandID, err)
	}

	// Build a map of existing tasks by NodeNamedVersion
	taskMap := make(map[api.NamedVersion]*api.Task)
	for i := range tasks {
		taskMap[tasks[i].NodeNamedVersion] = &tasks[i]
	}

	// Create a background context for handlers
	ctx := context.Background()

	// Process all registered nodes
	for nv, node := range m.nodes {
		if task, exists := taskMap[nv]; exists {
			// Task exists for this node - save previous state
			prevState := task.Status

			// Check state and update accordingly
			switch task.Status {
			case api.StatusIdle:
				// Do nothing - task is already idle
			case api.StatusWaiting:
				// Mark as idle
				task.Status = api.StatusIdle
			case api.StatusRunning:
				// Mark as idle
				task.Status = api.StatusIdle
			case api.StatusFailed:
				// Do nothing - leave failed tasks as-is
			case api.StatusDone:
				// Do nothing - leave completed tasks as-is
			}

			// Save task if status changed
			if task.Status != prevState {
				if err := m.db.Save(task).Error; err != nil {
					return nil, fmt.Errorf("failed to update task %d: %w", task.ID, err)
				}
			}

			// Call OnTaskLoad with the previous state
			if err := node.OnTaskLoad(ctx, &command, task, prevState); err != nil {
				return nil, fmt.Errorf("failed to call OnTaskLoad for task %d: %w", task.ID, err)
			}

			// Remove from map so we know it's been processed
			delete(taskMap, nv)
		} else {
			// No task exists for this node - create one in idle state
			newTask := &api.Task{
				CommandID:        commandID,
				NodeNamedVersion: nv,
				Status:           api.StatusIdle,
				Params:           node.GetDefaultTaskParams(&command),
				Orphan:           false,
			}
			if err := m.db.Create(newTask).Error; err != nil {
				return nil, fmt.Errorf("failed to create task for node %v: %w", nv, err)
			}

			// Call OnTaskCreate (enqueue=false for requeue)
			if err := node.OnTaskCreate(ctx, &command, newTask, false); err != nil {
				return nil, fmt.Errorf("failed to call OnTaskCreate for new task: %w", err)
			}
		}
	}

	// Any remaining tasks in taskMap are orphans (no matching node)
	for _, task := range taskMap {
		if !task.Orphan {
			task.Orphan = true
			if err := m.db.Save(task).Error; err != nil {
				return nil, fmt.Errorf("failed to mark task %d as orphan: %w", task.ID, err)
			}
		}
	}

	// Reload the command with tasks preloaded
	if err := m.db.Preload("Tasks").First(&command, commandID).Error; err != nil {
		return nil, fmt.Errorf("failed to reload command %d: %w", commandID, err)
	}

	return &command, nil
}

// Start starts the execution loop in a separate go routine.
func (m *mpat) Start() error {
	return nil
}

// Stop gracefully stops the queue.
func (m *mpat) Stop() error {
	return nil
}

// GetCurrentCommandID gets the current active command ID.
func (m *mpat) GetCurrentCommandID() (uint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.runningTask {
		return 0, fmt.Errorf("no task is currently running")
	}

	return m.currentCommandID, nil
}

// GetCommand gets the command with the given ID.
func (m *mpat) GetCommand(commandID uint) (*api.Command, error) {
	var command api.Command
	if err := m.db.Preload("Tasks").First(&command, commandID).Error; err != nil {
		return nil, fmt.Errorf("failed to find command %d: %w", commandID, err)
	}
	return &command, nil
}

// SetPriority sets the priority of a command.
func (m *mpat) SetPriority(commandID uint, p uint) error {
	// Get the command from database
	var command api.Command
	if err := m.db.First(&command, commandID).Error; err != nil {
		return fmt.Errorf("failed to find command %d: %w", commandID, err)
	}

	// Update priority
	command.Priority = p
	if err := m.db.Save(&command).Error; err != nil {
		return fmt.Errorf("failed to update priority for command %d: %w", commandID, err)
	}

	return nil
}
