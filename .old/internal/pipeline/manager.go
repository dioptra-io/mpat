package pipeline

import (
	"context"
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ClickHouseManager[T any] struct {
	BufferSize int
	client     *clientv3.NativeSQLClient

	ctx context.Context
}

func NewClickHouseManager[T any](ctx context.Context, client *clientv3.NativeSQLClient) *ClickHouseManager[T] {
	return &ClickHouseManager[T]{
		client:     client,
		BufferSize: config.DefaultStreamBufferSize,
		ctx:        ctx,
	}
}

func (m *ClickHouseManager[T]) DeleteThenCreate(deleteQ, createQ queries.Query) error {
	if err := m.Delete(deleteQ); err != nil {
		return err
	}

	return m.Create(createQ)
}

func (m *ClickHouseManager[T]) Create(q queries.Query) error {
	query, err := q.Query()
	if err != nil {
		return err
	}

	if err := m.client.Exec(m.ctx, query); err != nil {
		return fmt.Errorf("manager table creation failed: %w", err)
	}
	return nil
}

func (m *ClickHouseManager[T]) Delete(q queries.Query) error {
	query, err := q.Query()
	if err != nil {
		return err
	}

	if err := m.client.Exec(m.ctx, query); err != nil {
		return fmt.Errorf("manager table creation failed: %w", err)
	}
	return nil
}

func (m *ClickHouseManager[T]) Execute(q queries.Query) error {
	query, err := q.Query()
	if err != nil {
		return err
	}

	if err := m.client.Exec(m.ctx, query); err != nil {
		return fmt.Errorf("manager table execution failed: %w", err)
	}
	return nil
}
