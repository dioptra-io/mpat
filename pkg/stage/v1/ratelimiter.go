package v1

import (
	"context"
	"time"
)

type RateLimiterStage[T any] struct {
	inCh       <-chan T
	outCh      chan T
	errCh      chan<- error
	numWorkers int
	ticker     *time.Ticker
}

// This limits the elements passing to the next stage.
func NewRateLimiterStage[T, K any](maxRatePerSecond int64, inCh <-chan T, errCh chan<- error) Stager[T, T] {
	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(maxRatePerSecond))

	return &RateLimiterStage[T]{
		inCh:   inCh,
		outCh:  make(chan T), // unbuffered channel
		errCh:  errCh,
		ticker: ticker,
	}
}

func (w *RateLimiterStage[T]) Start(ctx context.Context) error {
	StartWorkersFromChannel(ctx, 1, w.inCh, w.outCh, w.errCh, w.workerFn, w.exitFn)
	return nil
}

func (w *RateLimiterStage[T]) Output() <-chan T {
	return w.outCh
}

func (w *RateLimiterStage[T]) workerFn(ctx context.Context, t T) ([]T, error) {
	select {
	case <-ctx.Done():
		return []T{}, nil
	case <-w.ticker.C:
		return []T{t}, nil
	}
}

func (w *RateLimiterStage[T]) exitFn(_ context.Context) {
	w.ticker.Stop()
}
