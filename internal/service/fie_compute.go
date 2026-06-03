package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "embed"

	"github.com/dioptra-io/ufuk-research/internal/schema"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

const (
	DefaultFIEChunkSize         = 1_000_000
	DefaultFIERTTResolution     = 0.1
	DefaultFIEPreparationPolicy = store.PreparationPolicyFail

	zeroCursor = "::"
)

//go:embed templates/fie_results_insert.tmpl
var fieInsertResultsTemplate string

//go:embed templates/fie_results_cursor.tmpl
var fieCursorResultsTemplate string

//go:embed templates/fie_resultslite_insert.tmpl
var fieInsertResultsLiteTemplate string

//go:embed templates/fie_resultslite_cursor.tmpl
var fieCursorResultsLiteTemplate string

type sourceSchema int

const (
	sourceSchemaResults sourceSchema = iota
	sourceSchemaResultsLite
)

type fieTemplateData struct {
	SourceDatabase string
	SourceTable    string
	DestDatabase   string
	DestTable      string
	ChunkSize      int
	RTTResolution  float64
	Cursor         string
}

// FIEComputeConfig holds the configuration for the FIE computation service.
type FIEComputeConfig struct {
	ChunkSize         int
	RTTResolution     float64
	PreparationPolicy store.PreparationPolicy
}

// DefaultFIEComputeConfig returns a FIEComputeConfig with sensible defaults.
func DefaultFIEComputeConfig() FIEComputeConfig {
	return FIEComputeConfig{
		ChunkSize:     DefaultFIEChunkSize,
		RTTResolution: DefaultFIERTTResolution,
	}
}

// FIEComputeService computes Forwarding Info Elements from a source results table.
type FIEComputeService struct {
	store  *store.Store
	config FIEComputeConfig
}

// NewFIEComputeService creates a new FIEComputeService with the given store and config.
func NewFIEComputeService(s *store.Store, config FIEComputeConfig) *FIEComputeService {
	return &FIEComputeService{
		store:  s,
		config: config,
	}
}

// Compute computes FIEs from source into dest.
func (f *FIEComputeService) Compute(ctx context.Context, source, dest store.DatabaseTable) error {
	// Step 1: Validate source schema and detect type.
	sourceSchema, err := f.store.TableSchema(ctx, source)
	if err != nil {
		return fmt.Errorf("fie: failed to get source schema: %w", err)
	}
	if sourceSchema == nil {
		return fmt.Errorf("fie: source table %s.%s does not exist", source.Database, source.Table)
	}

	var detectedSchema schema.Schema
	switch {
	case func() bool { ok, _ := schema.AreEquivalent(schema.ResultsSchema{}, sourceSchema, false); return ok }():
		detectedSchema = schema.ResultsSchema{}
	case func() bool { ok, _ := schema.AreEquivalent(schema.ResultsLiteSchema{}, sourceSchema, false); return ok }():
		detectedSchema = schema.ResultsLiteSchema{}
	default:
		missing, _ := schema.MissingColumns(schema.ResultsLiteSchema{}, sourceSchema)
		return fmt.Errorf("fie: source table %s.%s does not match any supported schema, missing columns: %v", source.Database, source.Table, missing)
	}

	// Step 2: Prepare destination table.
	if err := f.store.PrepareTable(ctx, f.config.PreparationPolicy, dest, schema.FIEsSchema{}); err != nil {
		return fmt.Errorf("fie: failed to prepare destination table: %w", err)
	}

	// Step 3: Run the keyset-paginated INSERT loop. NEEDS UPDATE
	cursor := zeroCursor
	chunk := 0
	totalRows := uint64(0)
	start := time.Now()

	for {
		chunkStart := time.Now()

		// Get the last prefix of this chunk for the next cursor.
		lastPrefix, err := f.fiesLastPrefix(ctx, source, cursor, detectedSchema)
		if err != nil {
			return fmt.Errorf("fie: failed to get last prefix for cursor %s: %w", cursor, err)
		}
		if lastPrefix == "" {
			break
		}

		// Count rows before insert.
		countBefore, err := f.store.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("fie: failed to count rows before chunk %d: %w", chunk, err)
		}

		// Insert the chunk.
		if err := f.insertChunk(ctx, source, dest, cursor, detectedSchema); err != nil {
			return fmt.Errorf("fie: failed to insert chunk %d (cursor=%s): %w", chunk, cursor, err)
		}

		// Count rows after insert.
		countAfter, err := f.store.RowCount(ctx, dest)
		if err != nil {
			return fmt.Errorf("fie: failed to count rows after chunk %d: %w", chunk, err)
		}

		rowsInserted := countAfter - countBefore
		chunk++
		totalRows += rowsInserted
		fmt.Printf("[chunk %d] cursor=%-24s last=%-24s rows=%d elapsed=%s total=%d\n",
			chunk, cursor, lastPrefix, rowsInserted, time.Since(chunkStart).Round(time.Millisecond), totalRows,
		)

		cursor = lastPrefix
	}

	fmt.Printf("done: %d chunks, %d rows, elapsed=%s\n", chunk, totalRows, time.Since(start).Round(time.Second))
	return nil
}

func (f *FIEComputeService) fiesLastPrefix(ctx context.Context, source store.DatabaseTable, cursor string, s schema.Schema) (string, error) {
	var tmpl string
	switch s.(type) {
	case schema.ResultsSchema:
		tmpl = fieCursorResultsTemplate
	case schema.ResultsLiteSchema:
		tmpl = fieCursorResultsLiteTemplate
	default:
		return "", fmt.Errorf("fie: unsupported source schema %s", s.SchemaName())
	}

	query, err := renderTemplate("fie_cursor", tmpl, fieTemplateData{
		SourceDatabase: source.Database,
		SourceTable:    source.Table,
		ChunkSize:      f.config.ChunkSize,
		Cursor:         cursor,
	})
	if err != nil {
		return "", fmt.Errorf("fie: failed to render cursor template: %w", err)
	}
	var lastPrefix string
	row := f.store.QueryRow(ctx, query)
	if err := row.Scan(&lastPrefix); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("fie: failed to scan last prefix: %w", err)
	}
	return lastPrefix, nil
}

func (f *FIEComputeService) insertChunk(ctx context.Context, source, dest store.DatabaseTable, cursor string, s schema.Schema) error {
	var tmpl string
	switch s.(type) {
	case schema.ResultsSchema:
		tmpl = fieInsertResultsTemplate
	case schema.ResultsLiteSchema:
		tmpl = fieInsertResultsLiteTemplate
	default:
		return fmt.Errorf("fie: unsupported source schema %s", s.SchemaName())
	}

	query, err := renderTemplate("fie_insert", tmpl, fieTemplateData{
		SourceDatabase: source.Database,
		SourceTable:    source.Table,
		DestDatabase:   dest.Database,
		DestTable:      dest.Table,
		ChunkSize:      f.config.ChunkSize,
		RTTResolution:  f.config.RTTResolution,
		Cursor:         cursor,
	})
	if err != nil {
		return fmt.Errorf("fie: failed to render insert template: %w", err)
	}
	if err := f.store.Exec(ctx, query); err != nil {
		return fmt.Errorf("fie: failed to execute insert: %w", err)
	}
	return nil
}
