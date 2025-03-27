package v1

import (
	"context"
	"time"
)

type RateLimiterWorker[T any] struct {
	inCh       <-chan T
	outCh      chan T
	errCh      chan<- error
	numWorkers int
	ticker     *time.Ticker
}

func NewRateLimiterWorker[T, K any](maxRatePerSecond int64, inCh <-chan T, errCh chan<- error) Worker[T, T] {
	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(maxRatePerSecond))

	return &RateLimiterWorker[T]{
		inCh:   inCh,
		outCh:  make(chan T), // unbuffered channel
		errCh:  errCh,
		ticker: ticker,
	}
}

func (w *RateLimiterWorker[T]) Start(ctx context.Context) error {
	StartWorkersFromChannel(ctx, 1, w.inCh, w.outCh, w.errCh, w.workerFn, w.exitFn)
	return nil
}

func (w *RateLimiterWorker[T]) Output() <-chan T {
	return w.outCh
}

func (w *RateLimiterWorker[T]) workerFn(ctx context.Context, t T) ([]T, error) {
	select {
	case <-ctx.Done():
		return []T{}, nil
	case <-w.ticker.C:
		return []T{t}, nil
	}
}

func (w *RateLimiterWorker[T]) exitFn(_ context.Context) {
	w.ticker.Stop()
}
