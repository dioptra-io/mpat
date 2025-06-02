package pipeline

import (
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ClickHouseManager[T any] struct {
	BufferSize int
	client     *clientv2.SQLClient
}

func NewClickHouseManager[T any](client *clientv2.SQLClient) *ClickHouseManager[T] {
	return &ClickHouseManager[T]{
		client:     client,
		BufferSize: config.DefaultStreamBufferSize,
	}
}

func (m *ClickHouseManager[T]) DeleteThenCreate(deleteFirst bool, deleteQ, createQ queries.Query) error {
	if deleteFirst {
		if err := m.Delete(deleteQ); err != nil {
			return err
		}
	}

	return m.Create(createQ)
}

func (m *ClickHouseManager[T]) Create(q queries.Query) error {
	var obj T
	q.Set(m.client, obj)
	query, err := q.Query()
	if err != nil {
		return err
	}

	_, err = m.client.Exec(query)
	if err != nil {
		return fmt.Errorf("manager table creation failed: %w", err)
	}
	return nil
}

func (m *ClickHouseManager[T]) Delete(q queries.Query) error {
	var obj T
	q.Set(m.client, obj)
	query, err := q.Query()
	if err != nil {
		return err
	}

	_, err = m.client.Exec(query)
	if err != nil {
		return fmt.Errorf("manager table creation failed: %w", err)
	}
	return nil
}
