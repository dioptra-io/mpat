package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ArkStreamer struct {
	bufferSize int
	client     *clientv3.ArkClient
	G          *errgroup.Group
	ctx        context.Context
}

func NewArkStreamer(ctx context.Context, client *clientv3.ArkClient) *ArkStreamer {
	g, ctx := errgroup.WithContext(ctx)

	return &ArkStreamer{
		client:     client,
		bufferSize: config.DefaultStreamBufferSize,
		G:          g,
		ctx:        ctx,
	}
}

func (s *ArkStreamer) Ingest(t time.Time, numParalel int) <-chan *apiv3.IrisResultsRow {
	outCh := make(chan *apiv3.IrisResultsRow, s.bufferSize)
	s.G.SetLimit(numParalel + 1)
	var counter int64 = 0

	s.G.Go(func() error {
		var urls []string
		var err error

		for i := 1; i <= config.DefaultMaxRetries; i++ {
			urls, err = s.client.GetWartURLs(s.ctx, t)
			if err == nil {
				break
			} else if i == config.DefaultMaxRetries {
				logger.Errorf("arkstreamer error on GetWartURLs, max retries (%d) reached: %s", i, err)
				return err
			} else {
				logger.Errorf("arkstreamer error on GetWartURLs: %s", err)
				time.Sleep(time.Second * 5)
			}
		}

		for _, url := range urls {
			s.G.Go(func() error {
				var currentCh <-chan *apiv3.IrisResultsRow
				var err error

				for i := 1; i <= config.DefaultMaxRetries; i++ {
					currentCh, err = s.client.StreamIrisResultsRows(s.ctx, url)
					if err == nil {
						break
					} else if i == config.DefaultMaxRetries {
						logger.Errorf("arkstreamer error on StreamIrisResultsRows, max retries (%d) reached: %s", i, err)
						return err
					} else {
						logger.Errorf("arkstreamer error on StreamIrisResultsRows: %s", err)
						time.Sleep(time.Second * config.DefaultArkRetryWaitSeconds)
					}
				}

				for obj := range currentCh {
					select {
					case <-s.ctx.Done():
						return s.ctx.Err()
					case outCh <- obj:
					}
				}

				splitUrl := strings.Split(url, "/")
				atomic.AddInt64(&counter, 1) // no race cond
				logger.Printf("Finished upload for wart file (%d/%d): %s.\n", counter, len(urls), splitUrl[len(splitUrl)-1])
				return nil
			})
		}
		return nil
	})

	go func() {
		_ = s.G.Wait()
		close(outCh)
	}()

	return outCh
}
