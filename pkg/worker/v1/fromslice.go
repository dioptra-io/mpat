package v1

import "context"

type FromSliceWorker[T any] struct {
	inSlice []T
	outCh   chan T
	errCh   chan<- error
}

func NewFromSliceWorker[T, K any](from []T, errCh chan<- error) Worker[T, T] {
	return &FromSliceWorker[T]{
		inSlice: from,
		outCh:   make(chan T),
		errCh:   errCh,
	}
}

func (w *FromSliceWorker[T]) Start(ctx context.Context) error {
	StartWorkersFromArray(ctx, 1, w.inSlice, w.outCh, w.errCh, w.workerFn, w.exitFn)
	return nil
}

func (w *FromSliceWorker[T]) Output() <-chan T {
	return w.outCh
}

func (w *FromSliceWorker[T]) workerFn(_ context.Context, t T) ([]T, error) {
	return []T{t}, nil
}

func (w *FromSliceWorker[T]) exitFn(ctx context.Context) {
}
