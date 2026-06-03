package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"text/template"
	"time"

	_ "embed"
)

const (
	DefaultFIEChunkSize      = 1_000_000
	DefaultIrisRTTResolution = 0.1
	zeroCursor               = "::"
)

//go:embed sql/fies_create.sql
var fiesDDLTemplate string

//go:embed sql/fies_insert.sql
var fiesInsertTemplate string

//go:embed sql/fies_cursor.sql
var fiesCursorTemplate string

type FiesConfig struct {
	SourceTable   string
	ChunkSize     int
	RTTResolution float64
}

func DefaultFiesConfig(sourceTable string) FiesConfig {
	return FiesConfig{
		SourceTable:   sourceTable,
		ChunkSize:     DefaultFIEChunkSize,
		RTTResolution: DefaultIrisRTTResolution,
	}
}

type fiesTemplateData struct {
	Database      string
	Table         string
	SourceTable   string
	RTTResolution float64
	ChunkSize     int
	Cursor        string
}

func FIESSchema(dest DatabaseTable) (string, error) {
	tmpl, err := template.New("fies").Parse(fiesDDLTemplate)
	if err != nil {
		return "", fmt.Errorf("store: failed to parse fies DDL template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, dest); err != nil {
		return "", fmt.Errorf("store: failed to render fies DDL template: %w", err)
	}

	return buf.String(), nil
}

// GenerateFies populates the fies table from the source table using
// keyset-paginated chunks. It runs sequentially to ensure correct
// sequence number assignment.
func (s *Store) GenerateFies(ctx context.Context, dest DatabaseTable, cfg FiesConfig) error {
	cursor := zeroCursor
	chunk := 0
	totalRows := uint64(0)
	start := time.Now()

	for {
		chunkStart := time.Now()

		// Count rows before insert.
		countBefore, err := s.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("fie: failed to count rows before chunk %d: %w", chunk, err)
		}

		// Get the last prefix of this chunk for the next cursor.
		// Returns empty string when there are no more chunks.
		lastPrefix, err := s.fiesLastPrefix(ctx, cfg, cursor)
		if err != nil {
			return fmt.Errorf("fie: failed to get last prefix for cursor %s: %w", cursor, err)
		}

		// Empty means no more prefixes — we are done.
		if lastPrefix == "" {
			break
		}

		// Insert the chunk.
		if err := s.insertFiesChunk(ctx, dest, cfg, cursor); err != nil {
			return fmt.Errorf("fie: failed to insert chunk %d (cursor=%s): %w", chunk, cursor, err)
		}

		// Count rows after insert.
		countAfter, err := s.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("fie: failed to count rows after chunk %d: %w", chunk, err)
		}

		rowsInserted := countAfter - countBefore
		chunk++
		totalRows += rowsInserted
		fmt.Printf(
			"[chunk %d] cursor=%-24s last=%-24s rows=%d elapsed=%s total=%d",
			chunk, cursor, lastPrefix, rowsInserted, time.Since(chunkStart).Round(time.Millisecond), totalRows,
		)

		cursor = lastPrefix
	}

	fmt.Printf("done: %d chunks, %d rows, elapsed=%s", chunk, totalRows, time.Since(start).Round(time.Second))
	return nil
}

// fiesLastPrefix returns the last probe_dst_prefix in the current chunk,
// or empty string if there are no more chunks.
func (s *Store) fiesLastPrefix(ctx context.Context, cfg FiesConfig, cursor string) (string, error) {
	query, err := renderTemplate("fies_cursor", fiesCursorTemplate, fiesTemplateData{
		SourceTable: cfg.SourceTable,
		ChunkSize:   cfg.ChunkSize,
		Cursor:      cursor,
	})
	if err != nil {
		return "", fmt.Errorf("fie: failed to render cursor template: %w", err)
	}

	var lastPrefix string
	row := s.conn.QueryRow(ctx, query)
	if err := row.Scan(&lastPrefix); err != nil {
		// No rows means no more chunks — we are done.
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("fie: failed to scan last prefix: %w", err)
	}
	return lastPrefix, nil
}

// insertFiesChunk runs the INSERT INTO fies SELECT ... for one chunk.
func (s *Store) insertFiesChunk(ctx context.Context, dest DatabaseTable, cfg FiesConfig, cursor string) error {
	query, err := renderTemplate("fies_insert", fiesInsertTemplate, fiesTemplateData{
		Database:      dest.Database,
		Table:         dest.Table,
		SourceTable:   cfg.SourceTable,
		RTTResolution: cfg.RTTResolution,
		ChunkSize:     cfg.ChunkSize,
		Cursor:        cursor,
	})
	if err != nil {
		return fmt.Errorf("fie: failed to render insert template: %w", err)
	}

	if err := s.Exec(ctx, query); err != nil {
		return fmt.Errorf("fie: failed to execute insert: %w", err)
	}

	return nil
}
