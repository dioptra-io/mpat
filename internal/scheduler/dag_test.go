package scheduler

import (
	"testing"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

// Helper function to create a named version
func nv(name string, version uint) api.NamedVersion {
	return api.NewNV(name, version)
}

// TestNewAdditiveDAG tests the constructor
func TestNewAdditiveDAG(t *testing.T) {
	dag := NewAdditiveDAG()
	if dag == nil {
		t.Fatal("NewAdditiveDAG returned nil")
	}

	// Should start with empty maps
	depthMap := dag.GetDepthMap()
	if len(depthMap) != 0 {
		t.Errorf("Expected empty depth map, got %d entries", len(depthMap))
	}

	reverseDepthMap := dag.GetReverseDepthMap()
	if len(reverseDepthMap) != 0 {
		t.Errorf("Expected empty reverse depth map, got %d entries", len(reverseDepthMap))
	}
}

// TestAddNodeSingle tests adding a single node with no dependencies
func TestAddNodeSingle(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)

	err := dag.AddNode(nodeA)
	if err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	// Check depth map
	depthMap := dag.GetDepthMap()
	nvA := nv("A", 1)
	if depth, exists := depthMap[nvA]; !exists {
		t.Error("Node A/v1 not found in depth map")
	} else if depth != 0 {
		t.Errorf("Expected depth 0 for root node, got %d", depth)
	}

	// Check reverse depth map
	reverseDepthMap := dag.GetReverseDepthMap()
	if nodes, exists := reverseDepthMap[0]; !exists {
		t.Error("Depth 0 not found in reverse depth map")
	} else if len(nodes) != 1 || nodes[0] != nvA {
		t.Errorf("Expected [A/v1] at depth 0, got %v", nodes)
	}
}

// TestAddNodeDuplicate tests that adding duplicate nodes returns error
func TestAddNodeDuplicate(t *testing.T) {
	dag := NewAdditiveDAG()
	node1 := NewMockNode("A", 1, nil)
	node2 := NewMockNode("A", 1, nil)

	err := dag.AddNode(node1)
	if err != nil {
		t.Fatalf("Failed to add first node: %v", err)
	}

	err = dag.AddNode(node2)
	if err == nil {
		t.Error("Expected error when adding duplicate node, got nil")
	}
}

// TestAddNodeWithNonExistentDependency tests that dependencies must exist
func TestAddNodeWithNonExistentDependency(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeB := NewMockNode("B", 1, nil)

	// Try to add B with dependency on non-existent A
	err := dag.AddNode(nodeB, nv("A", 1))
	if err == nil {
		t.Error("Expected error when adding node with non-existent dependency, got nil")
	}
}

// TestAddNodeWithDependency tests adding nodes with dependencies
func TestAddNodeWithDependency(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)
	nodeB := NewMockNode("B", 1, nil)

	// Add root node A
	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node A: %v", err)
	}

	// Add B depending on A
	if err := dag.AddNode(nodeB, nv("A", 1)); err != nil {
		t.Fatalf("Failed to add node B: %v", err)
	}

	// Check depths
	depthMap := dag.GetDepthMap()
	if depthMap[nv("A", 1)] != 0 {
		t.Errorf("Expected depth 0 for A, got %d", depthMap[nv("A", 1)])
	}
	if depthMap[nv("B", 1)] != 1 {
		t.Errorf("Expected depth 1 for B, got %d", depthMap[nv("B", 1)])
	}
}

// TestMultipleRootNodes tests DAG with multiple root nodes
func TestMultipleRootNodes(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)
	nodeB := NewMockNode("B", 1, nil)
	nodeC := NewMockNode("C", 1, nil)

	// Add three root nodes
	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node A: %v", err)
	}
	if err := dag.AddNode(nodeB); err != nil {
		t.Fatalf("Failed to add node B: %v", err)
	}
	if err := dag.AddNode(nodeC); err != nil {
		t.Fatalf("Failed to add node C: %v", err)
	}

	// All should be at depth 0
	depthMap := dag.GetDepthMap()
	for _, namedVer := range []api.NamedVersion{nv("A", 1), nv("B", 1), nv("C", 1)} {
		if depth := depthMap[namedVer]; depth != 0 {
			t.Errorf("Expected depth 0 for %s, got %d", namedVer.String(), depth)
		}
	}

	// Check reverse depth map maintains insertion order
	reverseDepthMap := dag.GetReverseDepthMap()
	nodes := reverseDepthMap[0]
	expected := []api.NamedVersion{nv("A", 1), nv("B", 1), nv("C", 1)}
	if len(nodes) != len(expected) {
		t.Errorf("Expected %d nodes at depth 0, got %d", len(expected), len(nodes))
	}
	for i, node := range nodes {
		if node != expected[i] {
			t.Errorf("Expected node %s at position %d, got %s", expected[i].String(), i, node.String())
		}
	}
}

// TestComplexDAG tests a more complex DAG structure
func TestComplexDAG(t *testing.T) {
	dag := NewAdditiveDAG()

	// Create nodes
	nodeA := NewMockNode("A", 1, nil)
	nodeB := NewMockNode("B", 1, nil)
	nodeC := NewMockNode("C", 1, nil)
	nodeD := NewMockNode("D", 1, nil)
	nodeE := NewMockNode("E", 1, nil)

	nvA := nv("A", 1)
	nvB := nv("B", 1)
	nvC := nv("C", 1)
	nvD := nv("D", 1)
	nvE := nv("E", 1)

	// Build DAG:
	//     A   B
	//     |\ /|
	//     | X |
	//     |/ \|
	//     C   D
	//      \ /
	//       E

	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node A: %v", err)
	}
	if err := dag.AddNode(nodeB); err != nil {
		t.Fatalf("Failed to add node B: %v", err)
	}
	if err := dag.AddNode(nodeC, nvA); err != nil {
		t.Fatalf("Failed to add node C: %v", err)
	}
	if err := dag.AddNode(nodeD, nvA, nvB); err != nil {
		t.Fatalf("Failed to add node D: %v", err)
	}
	if err := dag.AddNode(nodeE, nvC, nvD); err != nil {
		t.Fatalf("Failed to add node E: %v", err)
	}

	// Check depths
	depthMap := dag.GetDepthMap()
	expectedDepths := map[string]uint{
		"A": 0,
		"B": 0,
		"C": 1,
		"D": 1,
		"E": 2,
	}

	for nodeName, expectedDepth := range expectedDepths {
		nodeNV := nv(nodeName, 1)
		if depth := depthMap[nodeNV]; depth != expectedDepth {
			t.Errorf("Expected depth %d for %s, got %d", expectedDepth, nodeName, depth)
		}
	}

	// Check reverse depth map
	reverseDepthMap := dag.GetReverseDepthMap()

	// Level 0: A, B (insertion order)
	if nodes := reverseDepthMap[0]; len(nodes) != 2 || nodes[0] != nvA || nodes[1] != nvB {
		t.Errorf("Expected [A/v1 B/v1] at depth 0, got %v", nodes)
	}

	// Level 1: C, D (insertion order)
	if nodes := reverseDepthMap[1]; len(nodes) != 2 || nodes[0] != nvC || nodes[1] != nvD {
		t.Errorf("Expected [C/v1 D/v1] at depth 1, got %v", nodes)
	}

	// Level 2: E
	if nodes := reverseDepthMap[2]; len(nodes) != 1 || nodes[0] != nvE {
		t.Errorf("Expected [E/v1] at depth 2, got %v", nodes)
	}
}

// TestDepthWithMultiplePaths tests that depth is calculated as max path
func TestDepthWithMultiplePaths(t *testing.T) {
	dag := NewAdditiveDAG()

	// Create nodes
	nodeA := NewMockNode("A", 1, nil)
	nodeB := NewMockNode("B", 1, nil)
	nodeC := NewMockNode("C", 1, nil)
	nodeD := NewMockNode("D", 1, nil)

	nvA := nv("A", 1)
	nvB := nv("B", 1)
	nvC := nv("C", 1)
	nvD := nv("D", 1)

	// Build DAG:
	//     A
	//     |
	//     B
	//     |
	//     C
	//     |
	//     D
	// And also: A -> D (shortcut)
	// D should have depth 3 (longest path)

	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node A: %v", err)
	}
	if err := dag.AddNode(nodeB, nvA); err != nil {
		t.Fatalf("Failed to add node B: %v", err)
	}
	if err := dag.AddNode(nodeC, nvB); err != nil {
		t.Fatalf("Failed to add node C: %v", err)
	}
	if err := dag.AddNode(nodeD, nvC, nvA); err != nil {
		t.Fatalf("Failed to add node D: %v", err)
	}

	// D should have depth 3 (from A->B->C->D path, not A->D)
	depthMap := dag.GetDepthMap()
	if depth := depthMap[nvD]; depth != 3 {
		t.Errorf("Expected depth 3 for D (max path), got %d", depth)
	}
}

// TestGetNode tests retrieving nodes
func TestGetNode(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)
	nvA := nv("A", 1)

	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	// Test successful retrieval
	retrieved, err := dag.GetNode(nvA)
	if err != nil {
		t.Errorf("Failed to get node A: %v", err)
	}
	if retrieved.NamedVersion() != nvA {
		t.Errorf("Expected node A/v1, got %s", retrieved.NamedVersion().String())
	}

	// Test non-existent node
	_, err = dag.GetNode(nv("B", 1))
	if err == nil {
		t.Error("Expected error when getting non-existent node, got nil")
	}
}

// TestGetDepthMapImmutability tests that returned map is a copy
func TestGetDepthMapImmutability(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)
	nvA := nv("A", 1)

	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	// Get depth map and modify it
	depthMap1 := dag.GetDepthMap()
	depthMap1[nvA] = 999

	// Get depth map again - should not be affected
	depthMap2 := dag.GetDepthMap()
	if depthMap2[nvA] != 0 {
		t.Errorf("Depth map was mutated: expected 0, got %d", depthMap2[nvA])
	}
}

// TestGetReverseDepthMapImmutability tests that returned map is a deep copy
func TestGetReverseDepthMapImmutability(t *testing.T) {
	dag := NewAdditiveDAG()
	nodeA := NewMockNode("A", 1, nil)
	nodeB := NewMockNode("B", 1, nil)

	nvA := nv("A", 1)

	if err := dag.AddNode(nodeA); err != nil {
		t.Fatalf("Failed to add node A: %v", err)
	}
	if err := dag.AddNode(nodeB); err != nil {
		t.Fatalf("Failed to add node B: %v", err)
	}

	// Get reverse depth map and modify it
	reverseMap1 := dag.GetReverseDepthMap()
	reverseMap1[0][0] = nv("MODIFIED", 999)
	reverseMap1[0] = append(reverseMap1[0], nv("EXTRA", 1))

	// Get reverse depth map again - should not be affected
	reverseMap2 := dag.GetReverseDepthMap()
	if reverseMap2[0][0] != nvA {
		t.Errorf("Reverse depth map was mutated: expected A/v1, got %s", reverseMap2[0][0].String())
	}
	if len(reverseMap2[0]) != 2 {
		t.Errorf("Reverse depth map was mutated: expected length 2, got %d", len(reverseMap2[0]))
	}
}

// TestInsertionOrder tests that nodes maintain insertion order
func TestInsertionOrder(t *testing.T) {
	dag := NewAdditiveDAG()

	// Add nodes in specific order
	names := []string{"D", "A", "C", "B"}
	var expectedOrder []api.NamedVersion

	for _, name := range names {
		namedVer := nv(name, 1)
		node := NewMockNode(namedVer.Name, namedVer.Version, nil)
		if err := dag.AddNode(node); err != nil {
			t.Fatalf("Failed to add node %s: %v", name, err)
		}
		expectedOrder = append(expectedOrder, namedVer)
	}

	// Check that reverse depth map maintains insertion order
	reverseDepthMap := dag.GetReverseDepthMap()
	actualOrder := reverseDepthMap[0]

	if len(actualOrder) != len(expectedOrder) {
		t.Errorf("Expected %d nodes, got %d", len(expectedOrder), len(actualOrder))
	}

	for i, expected := range expectedOrder {
		if actualOrder[i] != expected {
			t.Errorf("Position %d: expected %s, got %s", i, expected.String(), actualOrder[i].String())
		}
	}
}

// TestEmptyDAG tests operations on empty DAG
func TestEmptyDAG(t *testing.T) {
	dag := NewAdditiveDAG()

	// GetNode should return error
	_, err := dag.GetNode(nv("A", 1))
	if err == nil {
		t.Error("Expected error when getting node from empty DAG")
	}

	// Maps should be empty
	depthMap := dag.GetDepthMap()
	if len(depthMap) != 0 {
		t.Errorf("Expected empty depth map, got %d entries", len(depthMap))
	}

	reverseDepthMap := dag.GetReverseDepthMap()
	if len(reverseDepthMap) != 0 {
		t.Errorf("Expected empty reverse depth map, got %d entries", len(reverseDepthMap))
	}
}
