package pipeline

import (
	"fmt"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ClickHouseRowStreamer[T any] struct {
	bufferSize      int
	egressChunkSize int
	client          *clientv3.NativeSQLClient
	G               *errgroup.Group
	ctx             context.Context
}

func NewClickHouseRowStreamer[T any](ctx context.Context, client *clientv3.NativeSQLClient) *ClickHouseRowStreamer[T] {
	g, ctx := errgroup.WithContext(ctx)

	return &ClickHouseRowStreamer[T]{
		bufferSize:      config.DefaultStreamBufferSize,
		egressChunkSize: config.DefaultUploadChunkSize,
		client:          client,
		G:               g,
		ctx:             ctx,
	}
}

func (s *ClickHouseRowStreamer[T]) Ingest(q queries.Query) <-chan *T {
	objCh := make(chan *T, s.bufferSize)

	s.G.Go(func() error {
		defer close(objCh)

		query, err := q.Query()
		if err != nil {
			return fmt.Errorf("streamer ingest query generation failed: %w", err)
		}

		rows, err := s.client.Query(s.ctx, query)
		if err != nil {
			return fmt.Errorf("streamer ingest query invokation filed: %w", err)
		}

		defer rows.Close()

		for rows.Next() {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			default:
			}

			objPointer := new(T)
			scannableFieldPointers, err := orm.GetFieldPointers(objPointer)
			if err != nil {
				return fmt.Errorf("streamer ingest field pointer computation failed: %w", err)
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("streamer ingest row iteration failed: %w", err)
			}

			if err := rows.Scan(scannableFieldPointers...); err != nil {
				return fmt.Errorf("streamer ingest row scan failed: %w", err)
			}

			objCh <- objPointer
		}
		return nil
	})

	return objCh
}

func (s *ClickHouseRowStreamer[T]) Egress(objCh <-chan *T, q queries.Query, uploadWorkers int) {
	for i := 0; i < uploadWorkers; i++ {
		s.G.Go(func() error {
			query, err := q.Query()
			if err != nil {
				return fmt.Errorf("streamer egress query invocation failed: %w", err)
			}

			batch, err := s.client.PrepareBatch(s.ctx, query)
			if err != nil {
				return err
			}

			counter := 0

			for obj := range objCh {
				select {
				case <-s.ctx.Done():
					return s.ctx.Err()
				default:
				}

				counter++

				insertableFields, err := orm.GetInsertableFields(obj)
				if err != nil {
					return fmt.Errorf("streamer egress insertable field computation failed: %w", err)
				}

				if err := batch.Append(insertableFields...); err != nil {
					return fmt.Errorf("streamer egress batch append failed: %w", err)
				}

				if counter%s.egressChunkSize == 0 {
					if err := batch.Send(); err != nil {
						return fmt.Errorf("sending batch send function: %w", err)
					}

					batch, err = s.client.PrepareBatch(s.ctx, query)
					if err != nil {
						return err
					}
				}

			}

			if counter%s.egressChunkSize != 0 {
				if err := batch.Send(); err != nil {
					return fmt.Errorf("sending batch send function: %w", err)
				}
			}

			return nil
		})
	}
}
