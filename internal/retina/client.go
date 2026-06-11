package retina

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	DefaultEndpoint  = "http://iprl.dioptra.io/api/v1/stream"
	DefaultBatchSize = 1000
)

// Config holds the configuration for a RetinaClient.
// Zero values are replaced with defaults when passed to NewRetinaClient.
type Config struct {
	// Endpoint is the URL of the Retina FIE stream.
	// Defaults to "http://iprl.dioptra.io/api/v1/stream".
	Endpoint string

	// BatchSize is the number of SequencedFIEs to accumulate before sending
	// a batch on the output channel.
	// Defaults to 1000.
	BatchSize int

	// HTTPClient is the HTTP client used for the streaming request.
	// Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// StreamResponse is the result type yielded by Stream. Exactly one of Batch
// or Err is set per value. When Err is set the channel is closed immediately
// after; no further values are sent.
type StreamResponse struct {
	Batch []SequencedFIE
	Err   error
}

// RetinaClient consumes the Retina FIE stream and delivers results in batches.
type RetinaClient struct {
	cfg Config
}

// NewRetinaClient returns a RetinaClient with defaults applied for any zero
// values in cfg.
func NewRetinaClient(cfg Config) *RetinaClient {
	if cfg.Endpoint == "" {
		cfg.Endpoint = DefaultEndpoint
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	return &RetinaClient{cfg: cfg}
}

// Stream opens the configured endpoint and returns a channel of StreamResponse.
// Each value carries either a batch of up to Config.BatchSize SequencedFIEs or
// a non-nil error. After an error the channel is closed; on EOF or context
// cancellation any partial batch accumulated so far is flushed before closing.
//
// Typical usage:
//
//	for r := range client.Stream(ctx) {
//	    if r.Err != nil {
//	        // handle error
//	    }
//	    // process r.Batch
//	}
func (c *RetinaClient) Stream(ctx context.Context) <-chan StreamResponse {
	ch := make(chan StreamResponse)

	go func() {
		defer close(ch)

		// send delivers r on ch. It always uses a background context so that a
		// partial-batch flush after cancellation is never dropped.
		send := func(r StreamResponse) {
			ch <- r
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.Endpoint, nil)
		if err != nil {
			send(StreamResponse{Err: fmt.Errorf("retina: build request: %w", err)})
			return
		}

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			send(StreamResponse{Err: fmt.Errorf("retina: connect: %w", err)})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			send(StreamResponse{Err: fmt.Errorf("retina: unexpected status %s", resp.Status)})
			return
		}

		batch := make([]SequencedFIE, 0, c.cfg.BatchSize)

		flush := func() {
			if len(batch) > 0 {
				send(StreamResponse{Batch: batch})
				batch = make([]SequencedFIE, 0, c.cfg.BatchSize)
			}
		}

		scanner := bufio.NewScanner(resp.Body)

		for scanner.Scan() {
			// On cancellation break out of the scan loop so we can flush below.
			select {
			case <-ctx.Done():
				flush()
				return
			default:
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			var fie SequencedFIE
			if err := json.Unmarshal(line, &fie); err != nil {
				flush()
				send(StreamResponse{Err: fmt.Errorf("retina: decode FIE: %w", err)})
				return
			}

			batch = append(batch, fie)

			if len(batch) >= c.cfg.BatchSize {
				flush()
			}
		}

		if err := scanner.Err(); err != nil {
			flush()
			send(StreamResponse{Err: fmt.Errorf("retina: read stream: %w", err)})
			return
		}

		flush()
	}()

	return ch
}
