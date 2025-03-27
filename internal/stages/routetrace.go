package stages

import (
	"context"
	"time"

	"github.com/ubombar/go-pipeline/pkg/stage"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
)

// This stage takes result table names and outputs RouteTraces.
type RouteTraceStage struct {
	inCh       <-chan *apiv1.RoutesTableInfo
	outCh      chan *apiv1.RoutesTableInfo
	errCh      chan<- error
	numWorkers int
	ticker     *time.Ticker
}

// This limits the elements passing to the next stage.
func NewRouteTraceStage[T, K any](maxRatePerSecond int64, inCh <-chan T, errCh chan<- error) stage.Stager[T, T] {
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
