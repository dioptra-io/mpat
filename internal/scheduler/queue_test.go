package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestNewLinkedQueue(t *testing.T) {
	q := NewLinkedQueue()
	if q == nil {
		t.Fatal("NewLinkedQueue returned nil")
	}
	if q.head != nil {
		t.Error("New queue should have nil head")
	}
	if q.cond == nil {
		t.Error("New queue should have initialized cond")
	}
}

func TestEnqueueDequeue_SingleElement(t *testing.T) {
	q := NewLinkedQueue()

	err := q.Enqueue(1, 10)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	id, priority := q.Dequeue()
	if id != 1 {
		t.Errorf("Expected id 1, got %d", id)
	}
	if priority != 10 {
		t.Errorf("Expected priority 10, got %d", priority)
	}
}

func TestEnqueueDequeue_PriorityOrder(t *testing.T) {
	q := NewLinkedQueue()

	// Enqueue in random order
	q.Enqueue(1, 5)
	q.Enqueue(2, 10)
	q.Enqueue(3, 3)
	q.Enqueue(4, 15)
	q.Enqueue(5, 7)

	// Should dequeue in priority order (highest first)
	expected := []struct {
		id       uint
		priority uint
	}{
		{4, 15},
		{2, 10},
		{5, 7},
		{1, 5},
		{3, 3},
	}

	for i, exp := range expected {
		id, priority := q.Dequeue()
		if id != exp.id {
			t.Errorf("Dequeue %d: expected id %d, got %d", i, exp.id, id)
		}
		if priority != exp.priority {
			t.Errorf("Dequeue %d: expected priority %d, got %d", i, exp.priority, priority)
		}
	}
}

func TestEnqueueDequeue_SamePriority(t *testing.T) {
	q := NewLinkedQueue()

	// Enqueue multiple elements with same priority
	q.Enqueue(1, 10)
	q.Enqueue(2, 10)
	q.Enqueue(3, 10)

	// All should be dequeued (order among same priority may vary)
	seen := make(map[uint]bool)
	for i := 0; i < 3; i++ {
		id, priority := q.Dequeue()
		if priority != 10 {
			t.Errorf("Expected priority 10, got %d", priority)
		}
		if id < 1 || id > 3 {
			t.Errorf("Unexpected id: %d", id)
		}
		if seen[id] {
			t.Errorf("Id %d dequeued twice", id)
		}
		seen[id] = true
	}
}

func TestDequeue_BlocksOnEmpty(t *testing.T) {
	q := NewLinkedQueue()

	dequeued := make(chan bool)

	// Start a goroutine that will block on Dequeue
	go func() {
		q.Dequeue()
		dequeued <- true
	}()

	// Give it time to block
	time.Sleep(50 * time.Millisecond)

	// Verify it hasn't dequeued yet
	select {
	case <-dequeued:
		t.Error("Dequeue should have blocked on empty queue")
	default:
		// Expected: still blocking
	}

	// Now enqueue something
	q.Enqueue(1, 10)

	// Should unblock quickly
	select {
	case <-dequeued:
		// Expected: unblocked
	case <-time.After(100 * time.Millisecond):
		t.Error("Dequeue should have unblocked after Enqueue")
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	q := NewLinkedQueue()
	const numGoroutines = 10
	const itemsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Multiple goroutines enqueuing concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(offset uint) {
			defer wg.Done()
			for j := uint(0); j < itemsPerGoroutine; j++ {
				id := offset*itemsPerGoroutine + j
				q.Enqueue(id, id%20) // Priorities 0-19
			}
		}(uint(i))
	}

	wg.Wait()

	// Verify all items can be dequeued
	seen := make(map[uint]bool)
	prevPriority := uint(20) // Start higher than max priority

	for i := 0; i < numGoroutines*itemsPerGoroutine; i++ {
		id, priority := q.Dequeue()

		// Check priority order (non-increasing)
		if priority > prevPriority {
			t.Errorf("Priority order violated: got %d after %d", priority, prevPriority)
		}
		prevPriority = priority

		// Check for duplicates
		if seen[id] {
			t.Errorf("Id %d dequeued twice", id)
		}
		seen[id] = true
	}

	if len(seen) != numGoroutines*itemsPerGoroutine {
		t.Errorf("Expected %d unique items, got %d", numGoroutines*itemsPerGoroutine, len(seen))
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	q := NewLinkedQueue()
	const numProducers = 5
	const numConsumers = 3
	const itemsPerProducer = 100
	const totalItems = numProducers * itemsPerProducer

	var wg sync.WaitGroup
	dequeued := make(map[uint]bool)
	var mu sync.Mutex

	// Start consumers - each will dequeue until all items are consumed
	wg.Add(numConsumers)
	itemsPerConsumer := totalItems / numConsumers
	remainder := totalItems % numConsumers

	for i := 0; i < numConsumers; i++ {
		itemsToDequque := itemsPerConsumer
		if i < remainder {
			itemsToDequque++ // Distribute the remainder to first few consumers
		}

		go func(itemCount int) {
			defer wg.Done()
			for j := 0; j < itemCount; j++ {
				id, _ := q.Dequeue()
				mu.Lock()
				if dequeued[id] {
					t.Errorf("Id %d dequeued twice", id)
				}
				dequeued[id] = true
				mu.Unlock()
			}
		}(itemsToDequque)
	}

	// Start producers
	wg.Add(numProducers)
	for i := 0; i < numProducers; i++ {
		go func(offset uint) {
			defer wg.Done()
			for j := uint(0); j < itemsPerProducer; j++ {
				id := offset*itemsPerProducer + j
				q.Enqueue(id, id%10)
			}
		}(uint(i))
	}

	wg.Wait()

	mu.Lock()
	if len(dequeued) != totalItems {
		t.Errorf("Expected %d items dequeued, got %d", totalItems, len(dequeued))
	}
	mu.Unlock()
}

func TestMultipleBlockedDequeuers(t *testing.T) {
	q := NewLinkedQueue()
	const numDequeuers = 5

	results := make(chan uint, numDequeuers)

	// Start multiple blocked dequeuers
	for i := 0; i < numDequeuers; i++ {
		go func() {
			id, _ := q.Dequeue()
			results <- id
		}()
	}

	// Give them time to block
	time.Sleep(50 * time.Millisecond)

	// Enqueue items one by one
	for i := uint(0); i < numDequeuers; i++ {
		q.Enqueue(i, 10)
		time.Sleep(10 * time.Millisecond) // Small delay between enqueues
	}

	// Collect all results
	seen := make(map[uint]bool)
	for i := 0; i < numDequeuers; i++ {
		select {
		case id := <-results:
			if seen[id] {
				t.Errorf("Id %d received twice", id)
			}
			seen[id] = true
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timeout waiting for dequeue results")
		}
	}

	if len(seen) != numDequeuers {
		t.Errorf("Expected %d unique results, got %d", numDequeuers, len(seen))
	}
}

func BenchmarkEnqueue(b *testing.B) {
	q := NewLinkedQueue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(uint(i), uint(i%100))
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := NewLinkedQueue()
	// Pre-fill the queue
	for i := 0; i < b.N; i++ {
		q.Enqueue(uint(i), uint(i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Dequeue()
	}
}

func BenchmarkConcurrentEnqueueDequeue(b *testing.B) {
	q := NewLinkedQueue()

	b.RunParallel(func(pb *testing.PB) {
		i := uint(0)
		for pb.Next() {
			if i%2 == 0 {
				q.Enqueue(i, i%100)
			} else {
				// Try to dequeue, but don't block if empty
				done := make(chan bool, 1)
				go func() {
					q.Dequeue()
					done <- true
				}()
				select {
				case <-done:
				case <-time.After(1 * time.Millisecond):
				}
			}
			i++
		}
	})
}
