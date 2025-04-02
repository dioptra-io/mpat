package pipe

import (
	"context"
	"sync"
)

// Get element T and produce one or more K. Return err in case of an error.
type WorkerFn[T, K any] func(context.Context, <-chan T, chan<- K) error

// This function is invoked when the worker is done.
type WorkerPostRunFn[T, K any] func(context.Context)

// This function is invoked when the worker is done.
type WorkerPreRunFn[T, K any] func(context.Context)

// Helper function for worker. Workers can only be cancelled by context.
func StartStage[T, K any](ctx context.Context, numWorkers int, inCh <-chan T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K], s WorkerPreRunFn[T, K], e WorkerPostRunFn[T, K]) {
	var wg sync.WaitGroup
	var startWg sync.WaitGroup
	wg.Add(numWorkers)
	startWg.Add(1)

	go func() {
		defer startWg.Done()
		s(ctx)
	}()

	for i := 0; i < numWorkers; i++ { // spin up the workers
		go runWorkerFromChannel(ctx, &wg, &startWg, inCh, outCh, errCh, f)
	}

	go func() {
		wg.Wait()
		close(outCh)
		e(ctx) // notify e
	}()
}

func runWorkerFromChannel[T, K any](ctx context.Context, wg *sync.WaitGroup, startWg *sync.WaitGroup, inCh <-chan T, outCh chan<- K, errCh chan<- error, f WorkerFn[T, K]) {
	defer wg.Done()
	startWg.Wait() // waits for start function to finish
	for {
		select {
		case <-ctx.Done():
			return // cancelled by context
		default:
			err := f(ctx, inCh, outCh)

			if ok := handleErr[T, K](errCh, err); !ok {
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
