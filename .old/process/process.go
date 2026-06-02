package process

import (
	"context"
	"errors"
)

type (
	PreRunFn        func(ctx context.Context) error
	RunFn[T, K any] func(ctx context.Context, inCh <-chan T, outch chan<- K) error
	PostRunFn       func(ctx context.Context) error
)

var (
	ErrCommunicationChannelsNil = errors.New("one of the communication channels is nil")
	ErrNumWorkersIsNegative     = errors.New("number of workers given is negative")
)

func SliceToChannel[T any](elements []T) chan T {
	ch := make(chan T, len(elements))
	for i := 0; i < len(elements); i++ {
		ch <- elements[i]
	}
	return ch
}

func Pop[T any](ctx context.Context, inCh <-chan T, errCh <-chan error) (T, bool) {
	var zero T

	select {
	case <-ctx.Done():
		return zero, false
	case item, ok := <-inCh:
		if !ok {
			return zero, false
		}
		return item, true
	}
}

func Push[T any](ctx context.Context, outCh chan<- T, errCh chan<- error, item T) bool {
	select {
	case <-ctx.Done():
		return false
	case outCh <- item: // assumed to be not closed
		return true
	}
}

func ContextValid(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}
