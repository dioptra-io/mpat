package queue

import (
	"errors"
	"sync"
)

var (
	ErrQueueAlreadyClosed = errors.New("tqueue is already closed")
	ErrInvalidQueueSize   = errors.New("tqueue size is invalid")
	ErrQueueIsDrained     = errors.New("tqueue is drained, no element left")
)

// TQueue is an high performance task queue for producer-consumer design. The enqueue and dequeue
// funcitons are blocking. Make sure to Finalize() the queue after producers are done.
type TQueue[T any] struct {
	mu         *sync.Mutex // used for preventing race conditions.
	cond       *sync.Cond  // cond for signalization.
	buffer     []T         // this is the ring buffer for performance reasons.
	head, tail int         // head, and tail pointer.
	draining   bool        // queue is not accepting new elements.
	capacity   int         // total number of elements on the buffer.
	length     int         // number of elements on the buffer.
}

func New[T any](capacity int) (*TQueue[T], error) {
	if capacity <= 0 {
		return nil, ErrInvalidQueueSize
	}

	mu := sync.Mutex{} // new mutex

	return &TQueue[T]{
		mu:       &mu,
		cond:     sync.NewCond(&mu),
		buffer:   make([]T, capacity),
		draining: false,
		capacity: capacity,
		head:     0,
		tail:     0,
		length:   0,
	}, nil
}

// Enqueue an object to the queue. Blocking id queue is full.
// If draining the queue, Enqueue() returns ErrQueueAlreadyClosed
func (q *TQueue[T]) Enqueue(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.length == q.capacity {
		// drain mode does not allow insertions
		if q.draining {
			return false
		}
		q.cond.Wait() // wait for someone to dequeue
	}

	if q.draining {
		return false
	}

	q.buffer[q.head] = item
	q.head = (q.head + 1) % q.capacity
	q.length++

	q.cond.Broadcast()
	return true
}

// Dequeue an object to the queue. Blocking if queue is not finalized.
func (q *TQueue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T

	for q.length == 0 {
		if q.draining {
			return zero, false
		}
		q.cond.Wait() // wait for someone to enqueue
	}

	item := q.buffer[q.tail] // shallow copy here
	q.tail = (q.tail + 1) % q.capacity
	q.length--

	q.cond.Broadcast()

	return item, true
}

// Queue does not accept new elements, but remeaning ones will be processed.
func (q *TQueue[T]) Drain() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.draining {
		return ErrQueueAlreadyClosed
	}

	q.draining = true

	return nil
}

// Get total space available.
func (q *TQueue[T]) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

// Get number of elements in the queue.
func (q *TQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.length
}
