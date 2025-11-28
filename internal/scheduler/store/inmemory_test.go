package store

import (
	"testing"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

func TestNewInMemoryStore(t *testing.T) {
	store, err := NewInMemoryStore()
	if err != nil {
		t.Fatalf("NewInMemoryStore failed: %v", err)
	}
	if store == nil {
		t.Fatal("NewInMemoryStore returned nil store")
	}
	if store.commands == nil {
		t.Error("commands map not initialized")
	}
	if store.tasks == nil {
		t.Error("tasks map not initialized")
	}
	if store.nextCommandID != 1 {
		t.Errorf("nextCommandID should be 1, got %d", store.nextCommandID)
	}
	if store.nextTaskID != 1 {
		t.Errorf("nextTaskID should be 1, got %d", store.nextTaskID)
	}
}

func TestCreateEmptyCommand(t *testing.T) {
	store, _ := NewInMemoryStore()

	cmd, err := store.CreateEmptyCommand()
	if err != nil {
		t.Fatalf("CreateEmptyCommand failed: %v", err)
	}
	if cmd == nil {
		t.Fatal("CreateEmptyCommand returned nil command")
	}
	if cmd.ID != 1 {
		t.Errorf("Expected ID 1, got %d", cmd.ID)
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
	if cmd2.ID != 2 {
		t.Errorf("Expected ID 2, got %d", cmd2.ID)
	}
}

func TestCreateEmptyTask(t *testing.T) {
	store, _ := NewInMemoryStore()

	task, err := store.CreateEmptyTask()
	if err != nil {
		t.Fatalf("CreateEmptyTask failed: %v", err)
	}
	if task == nil {
		t.Fatal("CreateEmptyTask returned nil task")
	}
	if task.ID != 1 {
		t.Errorf("Expected ID 1, got %d", task.ID)
	}
	if task.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}

	// Create another task to test ID increment
	task2, err := store.CreateEmptyTask()
	if err != nil {
		t.Fatalf("CreateEmptyTask failed: %v", err)
	}
	if task2.ID != 2 {
		t.Errorf("Expected ID 2, got %d", task2.ID)
	}
}

func TestUpdateCommand(t *testing.T) {
	store, _ := NewInMemoryStore()

	// Create a command
	cmd, _ := store.CreateEmptyCommand()

	// Update the command
	cmd.Status = api.CommandStatusActive
	cmd.Priority = 10
	cmd.Params = "test params"

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
}

func TestUpdateCommand_NotFound(t *testing.T) {
	store, _ := NewInMemoryStore()

	// Try to update non-existent command
	cmd := &api.Command{ID: 999}
	err := store.UpdateCommand(cmd)
	if err == nil {
		t.Error("Expected error when updating non-existent command")
	}
}

func TestUpdateCommand_AllStatuses(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestUpdateTask(t *testing.T) {
	store, _ := NewInMemoryStore()

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
}

func TestUpdateTask_NotFound(t *testing.T) {
	store, _ := NewInMemoryStore()

	// Try to update non-existent task
	task := &api.Task{ID: 999}
	err := store.UpdateTask(task)
	if err == nil {
		t.Error("Expected error when updating non-existent task")
	}
}

func TestUpdateTask_AllStatuses(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestLoadCommands(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestLoadTasks(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestFindTasksByCommandID(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestNumCommands(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestNumTasks(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestCommandLifecycle(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestTaskLifecycle(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestTaskSleepingState(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestTaskOrphanedState(t *testing.T) {
	store, _ := NewInMemoryStore()

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

func TestDataIsolation(t *testing.T) {
	store, _ := NewInMemoryStore()

	// Create and modify a command
	cmd, _ := store.CreateEmptyCommand()
	cmd.Priority = 100

	// Load commands and modify
	commands, _ := store.LoadCommands()
	commands[0].Priority = 999

	// Verify original is unchanged (defensive copy)
	commands2, _ := store.LoadCommands()

	// The first command should still have priority 100, not 999
	// because LoadCommands returns copies
	foundOriginal := false
	for _, c := range commands2 {
		if c.ID == cmd.ID && c.Priority == 100 {
			foundOriginal = true
			break
		}
	}
	if !foundOriginal {
		t.Error("Data isolation failed: external modification affected internal state")
	}
}

func TestConcurrentAccess(t *testing.T) {
	store, _ := NewInMemoryStore()

	const numGoroutines = 10
	const opsPerGoroutine = 100

	done := make(chan bool, numGoroutines*2)

	// Concurrent command creation
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < opsPerGoroutine; j++ {
				store.CreateEmptyCommand()
			}
			done <- true
		}()
	}

	// Concurrent task creation
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < opsPerGoroutine; j++ {
				store.CreateEmptyTask()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines*2; i++ {
		<-done
	}

	// Verify counts
	expectedCommands := uint(numGoroutines * opsPerGoroutine)
	expectedTasks := uint(numGoroutines * opsPerGoroutine)

	if store.NumCommands() != expectedCommands {
		t.Errorf("Expected %d commands, got %d", expectedCommands, store.NumCommands())
	}
	if store.NumTasks() != expectedTasks {
		t.Errorf("Expected %d tasks, got %d", expectedTasks, store.NumTasks())
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	store, _ := NewInMemoryStore()

	// Create initial data
	for i := 0; i < 10; i++ {
		cmd, _ := store.CreateEmptyCommand()
		cmd.Priority = uint(i)
		cmd.Status = api.CommandStatusActive
		store.UpdateCommand(cmd)
	}

	done := make(chan bool, 20)

	// Concurrent readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				store.LoadCommands()
				store.NumCommands()
			}
			done <- true
		}()
	}

	// Concurrent writers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				cmd, _ := store.CreateEmptyCommand()
				cmd.Priority = 999
				cmd.Status = api.CommandStatusInactive
				store.UpdateCommand(cmd)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should not panic and should have consistent state
	commands, _ := store.LoadCommands()
	if uint(len(commands)) != store.NumCommands() {
		t.Error("Inconsistent state after concurrent access")
	}
}
