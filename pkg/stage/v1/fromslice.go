package v1

import "context"

type FromSliceStage[T any] struct {
	inSlice []T
	outCh   chan T
	errCh   chan<- error
}

func NewFromSliceStage[T, K any](from []T, errCh chan<- error) Stager[T, T] {
	return &FromSliceStage[T]{
		inSlice: from,
		outCh:   make(chan T),
		errCh:   errCh,
	}
}

func (w *FromSliceStage[T]) Start(ctx context.Context) error {
	StartWorkersFromArray(ctx, 1, w.inSlice, w.outCh, w.errCh, w.workerFn, w.exitFn)
	return nil
}

func (w *FromSliceStage[T]) Output() <-chan T {
	return w.outCh
}

func (w *FromSliceStage[T]) workerFn(_ context.Context, t T) ([]T, error) {
	return []T{t}, nil
}

func (w *FromSliceStage[T]) exitFn(ctx context.Context) {
}
