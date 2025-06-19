package pipeline

import (
	"context"
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

	s.G.Go(func() error {
		urls, err := s.client.GetWartFiles(s.ctx, t)
		if err != nil {
			return err
		}

		for _, url := range urls {
			s.G.Go(func() error {
				currentCh, err := s.client.DownloadRouteTraces(s.ctx, url)
				if err != nil {
					return err
				}

				for obj := range currentCh {
					select {
					case <-s.ctx.Done():
						return s.ctx.Err()
					case outCh <- obj:
					}
				}
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
