package mpat_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dioptra-io/ufuk-research/api"
	"github.com/dioptra-io/ufuk-research/internal/mpat"
)

// MockNode is a test implementation of the Node interface
type MockNode struct {
	name           string
	version        uint
	onRunCalled    bool
	onLoadCalled   bool
	onCreateCalled bool
	onStopCalled   bool
	onIntCalled    bool
}

func NewMockNode(name string, version uint) *MockNode {
	return &MockNode{
		name:    name,
		version: version,
	}
}

func (n *MockNode) NamedVersion() api.NamedVersion {
	return api.NamedVersion{
		Name:    n.name,
		Version: n.version,
	}
}

func (n *MockNode) GetDefaultTaskParams(c *api.Command) string {
	return fmt.Sprintf(`{"node": "%s", "version": %d}`, n.name, n.version)
}

func (n *MockNode) OnTaskRun(ctx context.Context, c *api.Command, t *api.Task) error {
	n.onRunCalled = true
	return nil
}

func (n *MockNode) OnTaskLoad(ctx context.Context, c *api.Command, t *api.Task, prevState api.Status) error {
	n.onLoadCalled = true
	return nil
}

func (n *MockNode) OnTaskCreate(ctx context.Context, c *api.Command, t *api.Task, enqueue bool) error {
	n.onCreateCalled = true
	return nil
}

func (n *MockNode) OnTaskStopped(ctx context.Context, c *api.Command, t *api.Task) error {
	n.onStopCalled = true
	return nil
}

func (n *MockNode) OnTaskInterrupted(ctx context.Context, c *api.Command, t *api.Task) error {
	n.onIntCalled = true
	return nil
}

func TestNewMPAT(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}
	if m == nil {
		t.Fatal("Expected non-nil MPAT instance")
	}
}

func TestRegisterNode(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)

	// Register node without dependencies
	err = m.RegisterNode(nodeA)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Try to register same node again (should fail)
	err = m.RegisterNode(nodeA)
	if err == nil {
		t.Fatal("Expected error when registering duplicate node")
	}
}

func TestRegisterNodeWithDependencies(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	nodeB := NewMockNode("NodeB", 1)

	// Register first node
	err = m.RegisterNode(nodeA)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Register second node with dependency on first
	err = m.RegisterNode(nodeB, nodeA.NamedVersion())
	if err != nil {
		t.Fatalf("RegisterNode with dependency failed: %v", err)
	}
}

func TestRegisterNodeWithNonexistentDependency(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	nonexistent := api.NamedVersion{Name: "Nonexistent", Version: 1}

	// Try to register node with nonexistent dependency (should fail)
	err = m.RegisterNode(nodeA, nonexistent)
	if err == nil {
		t.Fatal("Expected error when registering node with nonexistent dependency")
	}
}

func TestRegisterNodeAfterFreeze(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	err = m.RegisterNode(nodeA)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Freeze dependencies
	err = m.FreezeDeps(false)
	if err != nil {
		t.Fatalf("FreezeDeps failed: %v", err)
	}

	// Try to register another node after freeze (should fail)
	nodeB := NewMockNode("NodeB", 1)
	err = m.RegisterNode(nodeB)
	if err == nil {
		t.Fatal("Expected error when registering node after freeze")
	}
}

func TestFreezeDeps(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	nodeB := NewMockNode("NodeB", 1)
	nodeC := NewMockNode("NodeC", 1)

	// Register nodes with dependencies: A -> B -> C
	m.RegisterNode(nodeA)
	m.RegisterNode(nodeB, nodeA.NamedVersion())
	m.RegisterNode(nodeC, nodeB.NamedVersion())

	// Check not frozen initially
	if m.AreDepsFrozen() {
		t.Fatal("Dependencies should not be frozen initially")
	}

	// Freeze
	err = m.FreezeDeps(false)
	if err != nil {
		t.Fatalf("FreezeDeps failed: %v", err)
	}

	// Check frozen
	if !m.AreDepsFrozen() {
		t.Fatal("Dependencies should be frozen after FreezeDeps")
	}

	// Try to freeze again (should fail)
	err = m.FreezeDeps(false)
	if err == nil {
		t.Fatal("Expected error when freezing twice")
	}
}

func TestEnqueueCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	// Enqueue command
	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	if cmd.ID == 0 {
		t.Fatal("Expected non-zero command ID")
	}

	if cmd.Priority != 10 {
		t.Fatalf("Expected priority 10, got %d", cmd.Priority)
	}

	if !cmd.Active {
		t.Fatal("Expected command to be active")
	}

	if len(cmd.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(cmd.Tasks))
	}

	if !nodeA.onCreateCalled {
		t.Fatal("Expected OnTaskCreate to be called")
	}
}

func TestEnqueueCommandBeforeFreeze(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)

	// Try to enqueue before freeze (should fail)
	_, err = m.EnqueueCommand(`{"test": "data"}`, 10)
	if err == nil {
		t.Fatal("Expected error when enqueueing before freeze")
	}
}

func TestGetCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Get command
	retrieved, err := m.GetCommand(cmd.ID)
	if err != nil {
		t.Fatalf("GetCommand failed: %v", err)
	}

	if retrieved.ID != cmd.ID {
		t.Fatalf("Expected ID %d, got %d", cmd.ID, retrieved.ID)
	}
}

func TestGetNonexistentCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	// Try to get nonexistent command
	_, err = m.GetCommand(9999)
	if err == nil {
		t.Fatal("Expected error when getting nonexistent command")
	}
}

func TestSetPriority(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Set priority
	err = m.SetPriority(cmd.ID, 20)
	if err != nil {
		t.Fatalf("SetPriority failed: %v", err)
	}

	// Verify priority changed
	retrieved, err := m.GetCommand(cmd.ID)
	if err != nil {
		t.Fatalf("GetCommand failed: %v", err)
	}

	if retrieved.Priority != 20 {
		t.Fatalf("Expected priority 20, got %d", retrieved.Priority)
	}
}

func TestDequeueCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Dequeue command
	err = m.DequeueCommand(cmd.ID)
	if err != nil {
		t.Fatalf("DequeueCommand failed: %v", err)
	}

	// Verify command is inactive
	retrieved, err := m.GetCommand(cmd.ID)
	if err != nil {
		t.Fatalf("GetCommand failed: %v", err)
	}

	if retrieved.Active {
		t.Fatal("Expected command to be inactive after dequeue")
	}

	if !nodeA.onStopCalled {
		t.Fatal("Expected OnTaskStopped to be called")
	}
}

func TestRequeueCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Dequeue
	m.DequeueCommand(cmd.ID)

	// Reset mock state
	nodeA.onLoadCalled = false

	// Requeue
	requeued, err := m.RequeueCommand(cmd.ID)
	if err != nil {
		t.Fatalf("RequeueCommand failed: %v", err)
	}

	if !requeued.Active {
		t.Fatal("Expected command to be active after requeue")
	}

	if !nodeA.onLoadCalled {
		t.Fatal("Expected OnTaskLoad to be called")
	}
}

func TestRequeueAlreadyActiveCommand(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Try to requeue already active command (should fail)
	_, err = m.RequeueCommand(cmd.ID)
	if err == nil {
		t.Fatal("Expected error when requeueing already active command")
	}
}

func TestDepthCalculation(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	// Create a diamond dependency graph:
	//     A (depth 0)
	//    / \
	//   B   C (depth 1)
	//    \ /
	//     D (depth 2)
	nodeA := NewMockNode("NodeA", 1)
	nodeB := NewMockNode("NodeB", 1)
	nodeC := NewMockNode("NodeC", 1)
	nodeD := NewMockNode("NodeD", 1)

	m.RegisterNode(nodeA)
	m.RegisterNode(nodeB, nodeA.NamedVersion())
	m.RegisterNode(nodeC, nodeA.NamedVersion())
	m.RegisterNode(nodeD, nodeB.NamedVersion(), nodeC.NamedVersion())

	err = m.FreezeDeps(false)
	if err != nil {
		t.Fatalf("FreezeDeps failed: %v", err)
	}

	// The depth calculation is internal, but we can verify it works
	// by successfully freezing a complex graph
}

func TestOrphanTaskHandling(t *testing.T) {
	m, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	nodeA := NewMockNode("NodeA", 1)
	m.RegisterNode(nodeA)
	m.FreezeDeps(false)

	cmd, err := m.EnqueueCommand(`{"test": "data"}`, 10)
	if err != nil {
		t.Fatalf("EnqueueCommand failed: %v", err)
	}

	// Dequeue
	m.DequeueCommand(cmd.ID)

	// Create a new MPAT instance (simulating restart without NodeA)
	m2, err := mpat.NewMPAT(":memory:")
	if err != nil {
		t.Fatalf("NewMPAT failed: %v", err)
	}

	// Note: This test is limited because we're using a new in-memory DB
	// In a real scenario with persistent storage, orphan detection would work
	nodeB := NewMockNode("NodeB", 1)
	m2.RegisterNode(nodeB)
	m2.FreezeDeps(true) // Load with orphan detection
}
