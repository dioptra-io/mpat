package mpat

import (
	"fmt"

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
