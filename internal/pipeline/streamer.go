package pipeline

import (
	"database/sql"
	"fmt"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ClickHouseStreamer[T any] struct {
	BufferSize      int
	EgressChunkSize int
	client          *clientv2.SQLClient
}

func NewClickHouseStreamer[T any](client *clientv2.SQLClient) *ClickHouseStreamer[T] {
	return &ClickHouseStreamer[T]{
		client:          client,
		BufferSize:      config.DefaultStreamBufferSize,
		EgressChunkSize: config.DefaultUploadChunkSize,
	}
}

func (s *ClickHouseStreamer[T]) Ingest(q queries.Query) (<-chan *T, <-chan error) {
	objCh := make(chan *T, s.BufferSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(objCh)
		defer close(errCh)
		var obj T

		q.Set(s.client, obj)
		query, err := q.Query()
		if err != nil {
			errCh <- fmt.Errorf("streamer ingest query generation failed: %w", err)
			return
		}

		rows, err := s.client.Query(query)
		if err != nil {
			errCh <- fmt.Errorf("streamer ingest query invokation filed: %w", err)
			return
		}

		defer rows.Close()

		for rows.Next() {
			objPointer := new(T)
			scannableFieldPointers, err := orm.GetFieldPointers(objPointer)
			if err != nil {
				errCh <- fmt.Errorf("streamer ingest field pointer computation failed: %w", err)
				return
			}

			if err := rows.Err(); err != nil {
				errCh <- fmt.Errorf("streamer ingest row iteration failed: %w", err)
				return
			}

			if err := rows.Scan(scannableFieldPointers...); err != nil {
				errCh <- fmt.Errorf("streamer ingest row scan failed: %w", err)
				return
			}

			objCh <- objPointer
		}
	}()

	return objCh, errCh
}

func (s *ClickHouseStreamer[T]) Egress(objCh <-chan *T, errCh <-chan error, q queries.Query) <-chan error {
	errCh2 := make(chan error, 1)

	go func() {
		defer close(errCh2)
		var tx *sql.Tx
		var stmt *sql.Stmt
		var err error
		var obj T

		q.Set(s.client, obj)
		query, err := q.Query()
		if err != nil {
			errCh2 <- fmt.Errorf("streamer egress query invokation failed: %w", err)
			return
		}

		beginTx := func() error {
			tx, err = s.client.Begin()
			if err != nil {
				return fmt.Errorf("streamer egress begin transaction failed: %w", err)
			}

			stmt, err = tx.Prepare(query)
			if err != nil {
				return fmt.Errorf("streamer egress prepare insert failed: %w", err)
			}
			return nil
		}

		commitTx := func() error {
			if stmt != nil {
				stmt.Close()
			}
			if tx != nil {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("streamer egress commit failed: %w", err)
				}
			}
			return nil
		}

		if err := beginTx(); err != nil {
			errCh2 <- err
			return
		}

		counter := 0

		for obj := range objCh {
			counter++
			insertableFields, err := orm.GetInsertableFields(obj)
			if err != nil {
				errCh2 <- fmt.Errorf("streamer egress insertable field computation failed: %w", err)
				return
			}

			if _, err := stmt.Exec(insertableFields...); err != nil {
				errCh2 <- fmt.Errorf("streamer egress insert exec failed: %w", err)
				return
			}

			if counter%s.EgressChunkSize == 0 {
				// Commit current chunk
				if err := commitTx(); err != nil {
					errCh2 <- err
					return
				}
				// Start new transaction
				if err := beginTx(); err != nil {
					errCh2 <- err
					return
				}
			}

			select {
			case err := <-errCh:
				errCh2 <- err
				return
			default:
			}
		}

		if err := commitTx(); err != nil {
			errCh2 <- err
			return
		}

		select {
		case err := <-errCh:
			errCh2 <- err
			return
		default:
		}
	}()

	return errCh2
}
