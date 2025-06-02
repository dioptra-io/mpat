package process

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
)

// StreamJSONL reads JSONL lines from r, unmarshals them into T, and sends them into out.
// It stops if the context is canceled. Errors are sent to errCh.
func StreamJSONL[T any](ctx context.Context, r io.Reader, errCh chan<- error) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				// continue
			}

			line := scanner.Bytes()
			var obj T
			if err := json.Unmarshal(line, &obj); err != nil {
				errCh <- err
				continue
			}

			select {
			case out <- obj:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}

		if err := scanner.Err(); err != nil {
			errCh <- err
		}
	}()
	return out
}
