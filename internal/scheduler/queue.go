package scheduler

import (
	"context"
	"sync"
)

// Queue represents a thread-safe command queue storing command IDs.
type Queue interface {
	// Enqueue adds a command ID with the given priority.
	Enqueue(ctx context.Context, id uint, p uint) error

	// Dequeue removes and returns the command ID with the highest priority. If the queue is empty, it blocks until an
	// element is available or context is cancelled.
	Dequeue(ctx context.Context) (uint, uint, error)

	// Remove specific commandID directlry.
	Remove(ctx context.Context, commandID uint) error
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
	head   *node
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
}

// NewLinkedQueue creates a new LinkedQueue
func NewLinkedQueue() *linkedQueue {
	lq := &linkedQueue{}
	lq.cond = sync.NewCond(&lq.mu)
	return lq
}

// Enqueue adds a command ID with the given priority. Elements are inserted in sorted order (highest priority first).
func (lq *linkedQueue) Enqueue(ctx context.Context, id uint, p uint) error {
	// Check context before acquiring lock
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	lq.mu.Lock()
	defer lq.mu.Unlock()

	if lq.closed {
		return context.Canceled
	}

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
// element is available or context is cancelled. Returns (0, 0) if context is cancelled.
func (lq *linkedQueue) Dequeue(ctx context.Context) (uint, uint, error) {
	lq.mu.Lock()
	defer lq.mu.Unlock()

	// Wait while queue is empty and context is not cancelled
	for lq.head == nil && !lq.closed {
		// Start a goroutine to watch for context cancellation
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				lq.mu.Lock()
				lq.closed = true
				lq.cond.Broadcast() // Wake up all waiting goroutines
				lq.mu.Unlock()
			case <-done:
				return
			}
		}()

		lq.cond.Wait()
		close(done) // Stop the context watcher

		// Check if we were woken up due to context cancellation
		if lq.closed && lq.head == nil {
			return 0, 0, ctx.Err()
		}
	}

	// If closed and no items, return context error
	if lq.head == nil {
		return 0, 0, ctx.Err()
	}

	// Remove and return the head (highest priority)
	node := lq.head
	lq.head = lq.head.next
	return node.id, node.priority, nil
}

// Remove deletes a specific command ID from the queue.
// If the command is not found, it returns nil.
// If the context is cancelled, it returns context.Canceled.
func (lq *linkedQueue) Remove(ctx context.Context, commandID uint) error {
	// Check context early
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	lq.mu.Lock()
	defer lq.mu.Unlock()

	if lq.closed {
		return context.Canceled
	}

	// Empty list
	if lq.head == nil {
		return nil
	}

	// If head matches
	if lq.head.id == commandID {
		lq.head = lq.head.next
		return nil
	}

	// Walk the linked list
	prev := lq.head
	curr := lq.head.next

	for curr != nil {
		// Check context while iterating
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if curr.id == commandID {
			prev.next = curr.next
			return nil
		}

		prev = curr
		curr = curr.next
	}

	// Not found — not an error
	return nil
}
