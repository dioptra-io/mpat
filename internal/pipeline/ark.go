package pipeline

import (
	"context"
	"time"

	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ArkStreamer struct {
	BufferSize int

	client *clientv2.ArkClient
}

func NewArkStreamer(client *clientv2.ArkClient) *ArkStreamer {
	return &ArkStreamer{
		client:     client,
		BufferSize: config.DefaultStreamBufferSize,
	}
}

func (s *ArkStreamer) Ingest(t time.Time) (<-chan *apiv3.IrisResultsRow, <-chan error) {
	objCh := make(chan *apiv3.IrisResultsRow, s.BufferSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(objCh)
		defer close(errCh)

		urls, err := s.client.GetWartFiles(context.Background(), t)
		if err != nil {
			errCh <- err
			return
		}

		// make this part parallel?
		for _, url := range urls {
			currentCh, err := s.client.DownloadRouteTraces(context.Background(), url)
			if err != nil {
				errCh <- err
				return
			}

			for obj := range currentCh {
				objCh <- obj
			}
		}
	}()

	return objCh, errCh
}
