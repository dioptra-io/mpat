package v1

import (
	"context"
	"sync"
)

// Worker represents a stage with n workers, consumes from InCh() and produces to OutCh().
// In case of an error it trys to write to ErrCh() and exists.
// In case of a context cancellation, it directly exists.
type Worker[T any, K any] interface {
	Start(ctx context.Context) error // start the workers

	Output() <-chan K // returns ro out channel
}

// Get element T and produce one or more K. Return err in case of an error.
type WorkerFn[T, K any] func(context.Context, T) ([]K, error)

// This function is invoked when the worker is done.
type WorkerExitFn[T, K any] func(context.Context)

// Helper function for worker. Workers can only be cancelled by context.
func StartWorkersFromChannel[T, K any](ctx context.Context, numWorkers int, inCh <-chan T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K], e WorkerExitFn[T, K]) {
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ { // spin up the workers
		go runWorkerFromChannel(ctx, &wg, inCh, outCh, errCh, f)
	}

	go func() {
		wg.Wait()
		close(outCh)
		e(ctx) // notify e
	}()
}

func runWorkerFromChannel[T, K any](ctx context.Context, wg *sync.WaitGroup, inCh <-chan T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K]) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return // cancelled by context
		case item, ok := <-inCh:
			if !ok {
				return // no more items to read
			}

			newItems, err := f(ctx, item)

			if ok := handleErr[T, K](errCh, err); !ok {
				return
			}

			if ok := handleOutput[T](ctx, outCh, newItems); !ok {
				return
			}
		}
	}
}

// Try to insert the error into the error channel. If err != nil return false.
func handleErr[T, K any](errCh chan<- error, err error) bool {
	if err != nil {
		select {
		case errCh <- err:
			return false // error inserted
		default:
			return false // errCh is full, return without reporting error
		}
	}

	return true
}

// Try to insert newItems to the outCh while checking for context cancellation.
func handleOutput[T, K any](ctx context.Context, outCh chan<- K, newItems []K) bool {
	for _, newItem := range newItems {
		select {
		case <-ctx.Done(): // check for context cancellation again
			return false
		case outCh <- newItem: // insert into out channel
			continue
		}
	}
	return true
}

func StartWorkersFromArray[T, K any](ctx context.Context, numWorkers int, inSlice []T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K], e WorkerExitFn[T, K]) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	counter := 0
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ { // spin up the workers
		go runWorkersFromArray(ctx, &wg, &mu, &counter, inSlice, outCh, errCh, f)
	}

	go func() {
		wg.Wait()
		close(outCh)
		e(ctx) // notify e
	}()
}

func runWorkersFromArray[T, K any](ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, counter *int, inSlice []T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K]) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return // cancelled by context
		default:
			item, ok := getItem(mu, counter, inSlice)

			if !ok {
				return
			}

			newItems, err := f(ctx, item)

			if ok := handleErr[T, K](errCh, err); !ok {
				return
			}

			if ok := handleOutput[T](ctx, outCh, newItems); !ok {
				return
			}
		}
	}
}

func getItem[T any](mu *sync.Mutex, counter *int, inSlice []T) (T, bool) {
	mu.Lock()
	defer mu.Unlock()
	var zero T

	if *counter >= len(inSlice) {
		return zero, false
	}

	*counter++
	return inSlice[*counter-1], true
}
