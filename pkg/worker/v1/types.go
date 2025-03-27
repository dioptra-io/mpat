package v1

import (
	"context"
	"sync"
)

// A worker consumes elements type T and produces elements type K.
type Worker[T any, K any] interface {
	Start(ctx context.Context) error // start the workers

	OutCh() chan K     // returns ro out channel
	InCh() chan T      // returns ro in channel
	ErrCh() chan error // returns ro err channel
}

// Get the context and a pointer T (for performance reasons) add many to write only to K. In case of an unrecoverable error
// return the error to stop pipeline.
type WorkerFunc[T, K any] func(context.Context, *T, chan<- *K) error

// Helper function for worker. Workers can only be cancelled by context.
func startWorkersFromChannel[T, K any](ctx context.Context, numWorkers int, inCh <-chan *T, outCh chan<- *K, errCh chan<- error, f WorkerFunc[T, K]) {
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return // cancelled by context
				case item, ok := <-inCh:
					if !ok {
						return // inCh closed
					}

					if err := f(ctx, item, outCh); err != nil {
						select {
						case errCh <- err:
							return // error inserted
						default:
							return // error is not inserted, maybe printerr?
						}
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
}

func startWorkersFromArray[T, K any](ctx context.Context, numWorkers int, inSlice []*T, outCh chan<- *K, errCh chan<- error, f WorkerFunc[T, K]) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	counter := 0
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return // cancelled by context
				default:
					item, ok := func() (*T, bool) {
						mu.Lock()
						defer mu.Unlock()

						if counter >= len(inSlice) {
							return nil, false
						}
						counter++
						return inSlice[counter-1], true
					}()

					if !ok {
						return
					}

					if err := f(ctx, item, outCh); err != nil {
						select {
						case errCh <- err:
							return // error inserted
						default:
							return // error is not inserted, maybe printerr?
						}
					}

				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outCh)
	}()
}
