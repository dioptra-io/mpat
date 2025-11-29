package scheduler

import (
	"fmt"
	"maps"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

// AdditiveDAG represents a Directed Acyclic Graph where nodes can only be added, not removed.
// This constraint ensures that once a node is added with its dependencies, the DAG structure
// remains valid and cycles cannot be introduced through removal operations.
type AdditiveDAG interface {
	// AddNode adds a node to the DAG with its dependencies.
	// The node is identified by its NamedVersion().
	// Returns an error if:
	//   - The node already exists in the DAG
	//   - Any of the specified dependencies do not exist in the DAG
	AddNode(n Node, deps ...api.NamedVersion) error

	// GetNode retrieves a node by its NamedVersion.
	// Returns an error if the node does not exist in the DAG.
	GetNode(nv api.NamedVersion) (Node, error)

	// GetDepthMap returns a map of each node's NamedVersion to its computed depth.
	// Depth represents the longest path from any root node (nodes with no dependencies).
	// Root nodes have depth 0.
	// Returns a copy of the internal map to prevent external modifications.
	GetDepthMap() map[api.NamedVersion]uint

	// GetReverseDepthMap returns a map of depths to the list of nodes at that depth.
	// Nodes at each depth level are ordered by their insertion order.
	// Returns a copy of the internal map to prevent external modifications.
	GetReverseDepthMap() map[uint][]api.NamedVersion

	// Returns all of the nodes as a list.
	GetNodes() []Node
}

// NewAdditiveDAG creates and returns a new AdditiveDAG instance.
func NewAdditiveDAG() AdditiveDAG {
	return newDAG()
}

// dag is the internal implementation of AdditiveDAG
type dag struct {
	// nodes maps node IDs to their Node interface implementations
	nodes map[api.NamedVersion]Node

	// dependencies maps each node ID to the list of node IDs it depends on
	// (i.e., nodeID -> [parent1, parent2, ...])
	dependencies map[api.NamedVersion][]api.NamedVersion

	// dependents maps each node ID to the list of node IDs that depend on it
	// (i.e., nodeID -> [child1, child2, ...])
	// This is the reverse of dependencies and is used for depth computation
	dependents map[api.NamedVersion][]api.NamedVersion

	// insertionOrder tracks the order in which nodes were added
	// Used to maintain insertion order in GetReverseDepthMap
	insertionOrder []api.NamedVersion

	// depthMap caches the computed depth for each node (NamedVersion -> depth)
	// Recomputed after each AddNode operation
	depthMap map[api.NamedVersion]uint

	// reverseDepthMap caches nodes grouped by depth (depth -> []NamedVersion)
	// Maintains insertion order within each depth level
	// Recomputed after each AddNode operation
	reverseDepthMap map[uint][]api.NamedVersion
}

var _ AdditiveDAG = (*dag)(nil)

// newDAG creates a new dag instance with initialized maps
func newDAG() *dag {
	return &dag{
		nodes:           make(map[api.NamedVersion]Node),
		dependencies:    make(map[api.NamedVersion][]api.NamedVersion),
		dependents:      make(map[api.NamedVersion][]api.NamedVersion),
		insertionOrder:  make([]api.NamedVersion, 0),
		depthMap:        make(map[api.NamedVersion]uint),
		reverseDepthMap: make(map[uint][]api.NamedVersion),
	}
}

// AddNode adds a node to the DAG with its dependencies
func (d *dag) AddNode(n Node, deps ...api.NamedVersion) error {
	nodeID := n.NamedVersion()

	// Check if node already exists
	if _, exists := d.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in the DAG", nodeID)
	}

	// Validate all dependencies exist
	for _, dep := range deps {
		if _, exists := d.nodes[dep]; !exists {
			return fmt.Errorf("dependency %s does not exist in the DAG", dep)
		}
	}

	// Add the node
	d.nodes[nodeID] = n
	d.insertionOrder = append(d.insertionOrder, nodeID)

	// Store dependencies
	if len(deps) > 0 {
		d.dependencies[nodeID] = make([]api.NamedVersion, len(deps))
		copy(d.dependencies[nodeID], deps)
	} else {
		d.dependencies[nodeID] = []api.NamedVersion{}
	}

	// Update reverse dependencies (dependents)
	for _, dep := range deps {
		d.dependents[dep] = append(d.dependents[dep], nodeID)
	}

	// Recompute depths
	d.computeDepths()

	return nil
}

// GetNode retrieves a node by its NamedVersion
func (d *dag) GetNode(nv api.NamedVersion) (Node, error) {
	node, exists := d.nodes[nv]
	if !exists {
		return nil, fmt.Errorf("node %s does not exist in the DAG", nv)
	}
	return node, nil
}

// GetDepthMap returns a copy of the depth map (NamedVersion -> depth)
func (d *dag) GetDepthMap() map[api.NamedVersion]uint {
	// Return a copy to prevent external modifications
	result := make(map[api.NamedVersion]uint, len(d.depthMap))
	maps.Copy(result, d.depthMap)
	return result
}

// GetReverseDepthMap returns a copy of the reverse depth map (depth -> []NamedVersion)
func (d *dag) GetReverseDepthMap() map[uint][]api.NamedVersion {
	// Return a deep copy to prevent external modifications
	result := make(map[uint][]api.NamedVersion, len(d.reverseDepthMap))
	for depth, nodes := range d.reverseDepthMap {
		nodesCopy := make([]api.NamedVersion, len(nodes))
		copy(nodesCopy, nodes)
		result[depth] = nodesCopy
	}
	return result
}

// computeDepths calculates the depth for each node in the DAG
// Depth is the longest path from any root node (nodes with no dependencies)
// Root nodes have depth 0
func (d *dag) computeDepths() {
	// Clear existing depth maps
	d.depthMap = make(map[api.NamedVersion]uint)
	d.reverseDepthMap = make(map[uint][]api.NamedVersion)

	// Find all root nodes (nodes with no dependencies)
	roots := make([]api.NamedVersion, 0)
	for nodeID := range d.nodes {
		if len(d.dependencies[nodeID]) == 0 {
			roots = append(roots, nodeID)
			d.depthMap[nodeID] = 0
		}
	}

	// BFS to compute depths
	queue := make([]api.NamedVersion, len(roots))
	copy(queue, roots)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		currentDepth := d.depthMap[current]

		// Process all dependents (children)
		for _, dependent := range d.dependents[current] {
			// Calculate new depth: max of all parent depths + 1
			newDepth := currentDepth + 1

			// If we've seen this node before, take the maximum depth
			if existingDepth, exists := d.depthMap[dependent]; exists {
				if newDepth > existingDepth {
					d.depthMap[dependent] = newDepth
				}
			} else {
				d.depthMap[dependent] = newDepth
				queue = append(queue, dependent)
			}
		}
	}

	// Build reverse depth map maintaining insertion order
	for _, nodeID := range d.insertionOrder {
		depth := d.depthMap[nodeID]
		d.reverseDepthMap[depth] = append(d.reverseDepthMap[depth], nodeID)
	}
}

// GetNodes returns all nodes in insertion order.
func (d *dag) GetNodes() []Node {
	out := make([]Node, 0, len(d.insertionOrder))
	for _, nv := range d.insertionOrder {
		if n, ok := d.nodes[nv]; ok {
			out = append(out, n)
		}
	}
	return out
}
