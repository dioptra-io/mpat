package scheduler

import "sync"

// Queue represents a thread-safe command queue storing command IDs.
type Queue interface {
	// Enqueue adds a command ID with the given priority.
	Enqueue(id uint, p uint) error

	// Dequeue removes and returns the command ID with the highest priority. If the queue is empty, it blocks until an
	// element is available.
	Dequeue() (uint, uint)
}

// node represents a single element in the priority queue
type node struct {
	id       uint
	priority uint
	next     *node
}

// linkedQueue is a thread-safe priority queue implemented as a sorted linked list. Higher priority values are dequeued
// first.
type linkedQueue struct {
	head *node
	mu   sync.Mutex
	cond *sync.Cond
}

// NewLinkedQueue creates a new LinkedQueue
func NewLinkedQueue() *linkedQueue {
	lq := &linkedQueue{}
	lq.cond = sync.NewCond(&lq.mu)
	return lq
}

// Enqueue adds a command ID with the given priority. Elements are inserted in sorted order (highest priority first).
func (lq *linkedQueue) Enqueue(id uint, p uint) error {
	lq.mu.Lock()
	defer lq.mu.Unlock()

	newNode := &node{
		id:       id,
		priority: p,
	}

	// Empty list or new node has highest priority
	if lq.head == nil || p > lq.head.priority {
		newNode.next = lq.head
		lq.head = newNode
		lq.cond.Signal() // Wake up any waiting Dequeue
		return nil
	}

	// Find the correct position to insert
	current := lq.head
	for current.next != nil && current.next.priority >= p {
		current = current.next
	}

	newNode.next = current.next
	current.next = newNode
	lq.cond.Signal() // Wake up any waiting Dequeue
	return nil
}

// Dequeue removes and returns the command ID with the highest priority. If the queue is empty, it blocks until an
// element is available.
func (lq *linkedQueue) Dequeue() (uint, uint) {
	lq.mu.Lock()
	defer lq.mu.Unlock()

	// Wait while queue is empty
	for lq.head == nil {
		lq.cond.Wait()
	}

	// Remove and return the head (highest priority)
	node := lq.head
	lq.head = lq.head.next
	return node.id, node.priority
}
