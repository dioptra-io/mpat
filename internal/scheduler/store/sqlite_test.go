package store

import (
	"os"
	"testing"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

func TestNewSQLiteStore(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	if store == nil {
		t.Fatal("NewSQLiteStore returned nil store")
	}
	if store.db == nil {
		t.Error("db not initialized")
	}
}

func TestNewSQLiteStore_FileDB(t *testing.T) {
	dbPath := "test.db"
	defer os.Remove(dbPath)

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.Close()

	// Verify file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestSQLiteStore_CreateEmptyCommand(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	cmd, err := store.CreateEmptyCommand()
	if err != nil {
		t.Fatalf("CreateEmptyCommand failed: %v", err)
	}
	if cmd == nil {
		t.Fatal("CreateEmptyCommand returned nil command")
	}
	if cmd.ID == 0 {
		t.Error("Command ID should be set by database")
	}
	if cmd.TaskIDs == nil {
		t.Error("TaskIDs should be initialized")
	}
	if len(cmd.TaskIDs) != 0 {
		t.Errorf("TaskIDs should be empty, got length %d", len(cmd.TaskIDs))
	}
	if cmd.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}

	// Create another command to test ID increment
	cmd2, err := store.CreateEmptyCommand()
	if err != nil {
		t.Fatalf("CreateEmptyCommand failed: %v", err)
	}
	if cmd2.ID <= cmd.ID {
		t.Errorf("Expected ID > %d, got %d", cmd.ID, cmd2.ID)
	}
}

func TestSQLiteStore_CreateEmptyTask(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	task, err := store.CreateEmptyTask()
	if err != nil {
		t.Fatalf("CreateEmptyTask failed: %v", err)
	}
	if task == nil {
		t.Fatal("CreateEmptyTask returned nil task")
	}
	if task.ID == 0 {
		t.Error("Task ID should be set by database")
	}
	if task.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}

	// Create another task to test ID increment
	task2, err := store.CreateEmptyTask()
	if err != nil {
		t.Fatalf("CreateEmptyTask failed: %v", err)
	}
	if task2.ID <= task.ID {
		t.Errorf("Expected ID > %d, got %d", task.ID, task2.ID)
	}
}

func TestSQLiteStore_UpdateCommand(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a command
	cmd, _ := store.CreateEmptyCommand()

	// Update the command
	cmd.Status = api.CommandStatusActive
	cmd.Priority = 10
	cmd.Params = "test params"
	cmd.TaskIDs = []uint{1, 2, 3}

	err := store.UpdateCommand(cmd)
	if err != nil {
		t.Fatalf("UpdateCommand failed: %v", err)
	}

	// Load and verify
	commands, _ := store.LoadCommands()
	if len(commands) != 1 {
		t.Fatalf("Expected 1 command, got %d", len(commands))
	}
	if commands[0].Status != api.CommandStatusActive {
		t.Errorf("Expected status Active, got %v", commands[0].Status)
	}
	if commands[0].Priority != 10 {
		t.Errorf("Expected priority 10, got %d", commands[0].Priority)
	}
	if commands[0].Params != "test params" {
		t.Errorf("Expected params 'test params', got %s", commands[0].Params)
	}
	if len(commands[0].TaskIDs) != 3 {
		t.Errorf("Expected 3 task IDs, got %d", len(commands[0].TaskIDs))
	}
}

func TestSQLiteStore_UpdateCommand_NotFound(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Try to update non-existent command
	cmd := &api.Command{ID: 999}
	err := store.UpdateCommand(cmd)
	if err == nil {
		t.Error("Expected error when updating non-existent command")
	}
}

func TestSQLiteStore_UpdateCommand_AllStatuses(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	statuses := []api.CommandStatus{
		api.CommandStatusActive,
		api.CommandStatusInactive,
		api.CommandStatusFailed,
		api.CommandStatusCompleted,
	}

	for _, status := range statuses {
		cmd, _ := store.CreateEmptyCommand()
		cmd.Status = status
		err := store.UpdateCommand(cmd)
		if err != nil {
			t.Errorf("Failed to update command with status %s: %v", status, err)
		}

		// Verify
		commands, _ := store.LoadCommands()
		found := false
		for _, c := range commands {
			if c.ID == cmd.ID && c.Status == status {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Command with status %s not found after update", status)
		}
	}
}

func TestSQLiteStore_UpdateTask(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a task
	task, _ := store.CreateEmptyTask()

	// Update the task
	task.Status = api.TaskStatusRunning
	task.CommandID = 1
	task.Params = "test params"
	task.NodeNV = api.NewNV("test_node", 2)

	err := store.UpdateTask(task)
	if err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	// Load and verify
	tasks, _ := store.LoadTasks()
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Status != api.TaskStatusRunning {
		t.Errorf("Expected status Running, got %v", tasks[0].Status)
	}
	if tasks[0].CommandID != 1 {
		t.Errorf("Expected CommandID 1, got %d", tasks[0].CommandID)
	}
	if tasks[0].NodeNV.Name != "test_node" {
		t.Errorf("Expected node name 'test_node', got %s", tasks[0].NodeNV.Name)
	}
	if tasks[0].NodeNV.Version != 2 {
		t.Errorf("Expected node version 2, got %d", tasks[0].NodeNV.Version)
	}
}

func TestSQLiteStore_UpdateTask_NotFound(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Try to update non-existent task
	task := &api.Task{ID: 999}
	err := store.UpdateTask(task)
	if err == nil {
		t.Error("Expected error when updating non-existent task")
	}
}

func TestSQLiteStore_UpdateTask_AllStatuses(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	statuses := []api.TaskStatus{
		api.TaskStatusReady,
		api.TaskStatusRunning,
		api.TaskStatusSleeping,
		api.TaskStatusFailed,
		api.TaskStatusCompleted,
		api.TaskStatusOrphaned,
	}

	for _, status := range statuses {
		task, _ := store.CreateEmptyTask()
		task.Status = status
		err := store.UpdateTask(task)
		if err != nil {
			t.Errorf("Failed to update task with status %s: %v", status, err)
		}

		// Verify
		tasks, _ := store.LoadTasks()
		found := false
		for _, tsk := range tasks {
			if tsk.ID == task.ID && tsk.Status == status {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Task with status %s not found after update", status)
		}
	}
}

func TestSQLiteStore_LoadCommands(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Empty store
	commands, err := store.LoadCommands()
	if err != nil {
		t.Fatalf("LoadCommands failed: %v", err)
	}
	if len(commands) != 0 {
		t.Errorf("Expected 0 commands, got %d", len(commands))
	}

	// Create some commands
	store.CreateEmptyCommand()
	store.CreateEmptyCommand()
	store.CreateEmptyCommand()

	commands, err = store.LoadCommands()
	if err != nil {
		t.Fatalf("LoadCommands failed: %v", err)
	}
	if len(commands) != 3 {
		t.Errorf("Expected 3 commands, got %d", len(commands))
	}
}

func TestSQLiteStore_LoadTasks(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Empty store
	tasks, err := store.LoadTasks()
	if err != nil {
		t.Fatalf("LoadTasks failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks, got %d", len(tasks))
	}

	// Create some tasks
	store.CreateEmptyTask()
	store.CreateEmptyTask()
	store.CreateEmptyTask()

	tasks, err = store.LoadTasks()
	if err != nil {
		t.Fatalf("LoadTasks failed: %v", err)
	}
	if len(tasks) != 3 {
		t.Errorf("Expected 3 tasks, got %d", len(tasks))
	}
}

func TestSQLiteStore_FindTasksByCommandID(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create tasks with different command IDs
	task1, _ := store.CreateEmptyTask()
	task1.CommandID = 1
	task1.Status = api.TaskStatusReady
	store.UpdateTask(task1)

	task2, _ := store.CreateEmptyTask()
	task2.CommandID = 1
	task2.Status = api.TaskStatusRunning
	store.UpdateTask(task2)

	task3, _ := store.CreateEmptyTask()
	task3.CommandID = 2
	task3.Status = api.TaskStatusCompleted
	store.UpdateTask(task3)

	// Find tasks for command 1
	tasks, err := store.FindTasksByCommandID(1)
	if err != nil {
		t.Fatalf("FindTasksByCommandID failed: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("Expected 2 tasks for command 1, got %d", len(tasks))
	}
	for _, task := range tasks {
		if task.CommandID != 1 {
			t.Errorf("Expected CommandID 1, got %d", task.CommandID)
		}
	}

	// Find tasks for command 2
	tasks, err = store.FindTasksByCommandID(2)
	if err != nil {
		t.Fatalf("FindTasksByCommandID failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Errorf("Expected 1 task for command 2, got %d", len(tasks))
	}
	if tasks[0].Status != api.TaskStatusCompleted {
		t.Errorf("Expected status Completed, got %v", tasks[0].Status)
	}

	// Find tasks for non-existent command
	tasks, err = store.FindTasksByCommandID(999)
	if err != nil {
		t.Fatalf("FindTasksByCommandID failed: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks for command 999, got %d", len(tasks))
	}
}

func TestSQLiteStore_NumCommands(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	if store.NumCommands() != 0 {
		t.Errorf("Expected 0 commands, got %d", store.NumCommands())
	}

	store.CreateEmptyCommand()
	if store.NumCommands() != 1 {
		t.Errorf("Expected 1 command, got %d", store.NumCommands())
	}

	store.CreateEmptyCommand()
	store.CreateEmptyCommand()
	if store.NumCommands() != 3 {
		t.Errorf("Expected 3 commands, got %d", store.NumCommands())
	}
}

func TestSQLiteStore_NumTasks(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	if store.NumTasks() != 0 {
		t.Errorf("Expected 0 tasks, got %d", store.NumTasks())
	}

	store.CreateEmptyTask()
	if store.NumTasks() != 1 {
		t.Errorf("Expected 1 task, got %d", store.NumTasks())
	}

	store.CreateEmptyTask()
	store.CreateEmptyTask()
	if store.NumTasks() != 3 {
		t.Errorf("Expected 3 tasks, got %d", store.NumTasks())
	}
}

func TestSQLiteStore_CommandLifecycle(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a command and simulate its lifecycle
	cmd, _ := store.CreateEmptyCommand()

	// Start as active
	cmd.Status = api.CommandStatusActive
	store.UpdateCommand(cmd)

	commands, _ := store.LoadCommands()
	if commands[0].Status != api.CommandStatusActive {
		t.Errorf("Expected Active status, got %v", commands[0].Status)
	}

	// Move to inactive
	cmd.Status = api.CommandStatusInactive
	store.UpdateCommand(cmd)

	commands, _ = store.LoadCommands()
	if commands[0].Status != api.CommandStatusInactive {
		t.Errorf("Expected Inactive status, got %v", commands[0].Status)
	}

	// Complete successfully
	cmd.Status = api.CommandStatusCompleted
	store.UpdateCommand(cmd)

	commands, _ = store.LoadCommands()
	if commands[0].Status != api.CommandStatusCompleted {
		t.Errorf("Expected Completed status, got %v", commands[0].Status)
	}
}

func TestSQLiteStore_TaskLifecycle(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a task and simulate its lifecycle
	task, _ := store.CreateEmptyTask()
	task.CommandID = 1

	// Start as ready
	task.Status = api.TaskStatusReady
	store.UpdateTask(task)

	tasks, _ := store.LoadTasks()
	if tasks[0].Status != api.TaskStatusReady {
		t.Errorf("Expected Ready status, got %v", tasks[0].Status)
	}

	// Move to running
	task.Status = api.TaskStatusRunning
	store.UpdateTask(task)

	tasks, _ = store.LoadTasks()
	if tasks[0].Status != api.TaskStatusRunning {
		t.Errorf("Expected Running status, got %v", tasks[0].Status)
	}

	// Complete successfully
	task.Status = api.TaskStatusCompleted
	store.UpdateTask(task)

	tasks, _ = store.LoadTasks()
	if tasks[0].Status != api.TaskStatusCompleted {
		t.Errorf("Expected Completed status, got %v", tasks[0].Status)
	}
}

func TestSQLiteStore_TaskSleepingState(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a task
	task, _ := store.CreateEmptyTask()
	task.CommandID = 1
	task.Status = api.TaskStatusRunning
	store.UpdateTask(task)

	// Put it to sleep
	task.Status = api.TaskStatusSleeping
	store.UpdateTask(task)

	tasks, _ := store.LoadTasks()
	if tasks[0].Status != api.TaskStatusSleeping {
		t.Errorf("Expected Sleeping status, got %v", tasks[0].Status)
	}

	// Wake it up
	task.Status = api.TaskStatusReady
	store.UpdateTask(task)

	tasks, _ = store.LoadTasks()
	if tasks[0].Status != api.TaskStatusReady {
		t.Errorf("Expected Ready status, got %v", tasks[0].Status)
	}
}

func TestSQLiteStore_TaskOrphanedState(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create a task
	task, _ := store.CreateEmptyTask()
	task.CommandID = 1
	task.Status = api.TaskStatusReady
	task.NodeNV = api.NewNV("missing_node", 1)
	store.UpdateTask(task)

	// Mark as orphaned
	task.Status = api.TaskStatusOrphaned
	store.UpdateTask(task)

	tasks, _ := store.LoadTasks()
	if tasks[0].Status != api.TaskStatusOrphaned {
		t.Errorf("Expected Orphaned status, got %v", tasks[0].Status)
	}
}

func TestSQLiteStore_Persistence(t *testing.T) {
	dbPath := "test_persistence.db"
	defer os.Remove(dbPath)

	// Create store and add data
	store1, _ := NewSQLiteStore(dbPath)
	cmd, _ := store1.CreateEmptyCommand()
	cmd.Status = api.CommandStatusActive
	cmd.Priority = 42
	store1.UpdateCommand(cmd)

	task, _ := store1.CreateEmptyTask()
	task.Status = api.TaskStatusReady
	task.CommandID = cmd.ID
	store1.UpdateTask(task)

	store1.Close()

	// Reopen and verify data persisted
	store2, _ := NewSQLiteStore(dbPath)
	defer store2.Close()

	commands, _ := store2.LoadCommands()
	if len(commands) != 1 {
		t.Fatalf("Expected 1 command after reopening, got %d", len(commands))
	}
	if commands[0].Priority != 42 {
		t.Errorf("Expected priority 42, got %d", commands[0].Priority)
	}

	tasks, _ := store2.LoadTasks()
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task after reopening, got %d", len(tasks))
	}
	if tasks[0].CommandID != cmd.ID {
		t.Errorf("Expected CommandID %d, got %d", cmd.ID, tasks[0].CommandID)
	}
}

func TestSQLiteStore_TaskIDsSerialization(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create command with task IDs
	cmd, _ := store.CreateEmptyCommand()
	cmd.TaskIDs = []uint{1, 2, 3, 4, 5}
	store.UpdateCommand(cmd)

	// Load and verify
	commands, _ := store.LoadCommands()
	if len(commands) != 1 {
		t.Fatalf("Expected 1 command, got %d", len(commands))
	}
	if len(commands[0].TaskIDs) != 5 {
		t.Errorf("Expected 5 task IDs, got %d", len(commands[0].TaskIDs))
	}
	for i, id := range commands[0].TaskIDs {
		if id != uint(i+1) {
			t.Errorf("Expected task ID %d at index %d, got %d", i+1, i, id)
		}
	}

	// Update with empty array
	cmd.TaskIDs = []uint{}
	store.UpdateCommand(cmd)

	commands, _ = store.LoadCommands()
	if len(commands[0].TaskIDs) != 0 {
		t.Errorf("Expected 0 task IDs, got %d", len(commands[0].TaskIDs))
	}
}

func TestSQLiteStore_NamedVersionSerialization(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()

	// Create task with NamedVersion
	task, _ := store.CreateEmptyTask()
	task.NodeNV = api.NewNV("processing_node", 5)
	store.UpdateTask(task)

	// Load and verify
	tasks, _ := store.LoadTasks()
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}
	if tasks[0].NodeNV.Name != "processing_node" {
		t.Errorf("Expected node name 'processing_node', got %s", tasks[0].NodeNV.Name)
	}
	if tasks[0].NodeNV.Version != 5 {
		t.Errorf("Expected node version 5, got %d", tasks[0].NodeNV.Version)
	}
}

func TestSQLiteStore_Close(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")

	err := store.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations after close should fail
	_, err = store.CreateEmptyCommand()
	if err == nil {
		t.Error("Expected error when using store after Close")
	}
}
