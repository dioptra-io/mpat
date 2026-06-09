package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/iris"
	"github.com/dioptra-io/ufuk-research/internal/schema"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

const (
	DefaultFetchChunkSize              = 500_000
	DefaultFetchTablePreparationPolicy = store.PreparationPolicyFail
	DefaultFetchLiteSchema             = true
)

// tableInfo holds pre-scanned metadata for a source table.
type tableInfo struct {
	name   string
	total  int64
	chunks int64
}

// FetchConfig holds the configuration for the fetch service.
type FetchConfig struct {
	ChunkSize         int
	PreparationPolicy store.PreparationPolicy
	Lite              bool // if true, uses ResultsLiteSchema, otherwise ResultsSchema
	EWMAAlpha         float64
	IPVersion         uint8 // 0 = both, 4 = IPv4 only, 6 = IPv6 only
}

// DefaultFetchConfig returns a FetchConfig with sensible defaults.
func DefaultFetchConfig() FetchConfig {
	return FetchConfig{
		ChunkSize:         DefaultFetchChunkSize,
		PreparationPolicy: DefaultFetchTablePreparationPolicy,
		Lite:              DefaultFetchLiteSchema,
		EWMAAlpha:         0.2,
	}
}

// FetchService fetches Iris results into a local ClickHouse table.
type FetchService struct {
	store      *store.Store
	irisClient *iris.IrisClient
	config     FetchConfig
}

// NewFetchService creates a new FetchService with the given store, iris client and config.
func NewFetchService(s *store.Store, irisClient *iris.IrisClient, cfg FetchConfig) *FetchService {
	return &FetchService{
		store:      s,
		irisClient: irisClient,
		config:     cfg,
	}
}

// targetSchema returns the schema to use based on the Lite config flag.
func (f *FetchService) targetSchema() schema.Schema {
	if f.config.Lite {
		return schema.ResultsLiteSchema{}
	}
	return schema.ResultsSchema{}
}

// Fetch fetches data from the given source tables into dest.
func (f *FetchService) Fetch(ctx context.Context, sourceNames []string, dest store.DatabaseTable) error {
	log := slog.Default()
	targetSchema := f.targetSchema()

	// Build column list from schema — only non-materialized columns.
	cols, err := targetSchema.Columns()
	if err != nil {
		return fmt.Errorf("fetch: failed to get columns for schema %s: %w", targetSchema.SchemaName(), err)
	}
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		if !col.Materialized {
			colNames = append(colNames, col.Name)
		}
	}
	selectCols := strings.Join(colNames, ", ")

	// Step 1: Pre-scan source tables.
	tables := make([]tableInfo, 0, len(sourceNames))
	totalChunks := int64(0)
	where := f.ipVersionFilter()
	for _, name := range sourceNames {
		total, err := countSourceRows(f.irisClient, name, where)
		if err != nil {
			return fmt.Errorf("fetch: failed to count rows in %s: %w", name, err)
		}
		chunks := (total + int64(f.config.ChunkSize) - 1) / int64(f.config.ChunkSize)
		if chunks == 0 {
			chunks = 1
		}
		tables = append(tables, tableInfo{name: name, total: total, chunks: chunks})
		totalChunks += chunks
	}

	log.InfoContext(ctx, "pre-scan complete",
		"tables", len(tables),
		"total_chunks", totalChunks,
		"policy", f.config.PreparationPolicy,
		"schema", targetSchema.SchemaName(),
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
	)

	// Step 2: Prepare destination table.
	if err := f.store.PrepareTable(ctx, f.config.PreparationPolicy, dest, targetSchema); err != nil {
		return fmt.Errorf("fetch: failed to prepare destination table: %w", err)
	}

	// Check if the existing table's schema is equivalent to the target schema.
	existingSchema, err := f.store.TableSchema(ctx, dest)
	if err != nil {
		return fmt.Errorf("fetch: failed to get existing table schema: %w", err)
	}
	if existingSchema != nil {
		ok, err := schema.AreEquivalent(targetSchema, existingSchema, false)
		if err != nil {
			return fmt.Errorf("fetch: failed to compare schemas: %w", err)
		}
		if !ok {
			missing, _ := schema.MissingColumns(targetSchema, existingSchema)
			extra, _ := schema.MissingColumns(existingSchema, targetSchema)
			return fmt.Errorf("fetch: destination table %s.%s schema does not match %s, missing columns: %v, extra columns: %v",
				dest.Database, dest.Table, targetSchema.SchemaName(), missing, extra)
		}
	}

	// Step 3: Fetch and write chunks.
	var ewmaRate float64
	globalChunk := int64(0)
	start := time.Now()

	for i, t := range tables {
		log.InfoContext(ctx, "fetching table",
			"table", t.name,
			"rows", t.total,
			"chunks", t.chunks,
			"progress", fmt.Sprintf("%d/%d", i+1, len(tables)),
		)

		for c := int64(0); c < t.chunks; c++ {
			offset := c * int64(f.config.ChunkSize)

			globalChunk++
			chunkRows := int64(f.config.ChunkSize)
			if remaining := t.total - offset; remaining < chunkRows {
				chunkRows = remaining
			}

			sql := fmt.Sprintf("SELECT %s FROM %s", selectCols, t.name)
			if where != "" {
				sql += " WHERE " + where
			}
			sql += fmt.Sprintf(" LIMIT %d OFFSET %d", f.config.ChunkSize, offset)
			rows, err := f.irisClient.Query().Select(sql).Json()
			if err != nil {
				return fmt.Errorf("[%d/%d] chunk %d: failed to query: %w", i+1, len(tables), c+1, err)
			}

			chunkStart := time.Now()
			if err := f.store.InsertJSONL(dest, rows); err != nil {
				return fmt.Errorf("[%d/%d] chunk %d: failed to write: %w", i+1, len(tables), c+1, err)
			}
			elapsed := time.Since(chunkStart)

			// Update EWMA rate (rows/sec).
			currentRate := float64(chunkRows) / elapsed.Seconds()
			if ewmaRate == 0 {
				ewmaRate = currentRate
			} else {
				ewmaRate = f.config.EWMAAlpha*currentRate + (1-f.config.EWMAAlpha)*ewmaRate
			}

			// Compute ETA from remaining chunks globally.
			remainingChunks := totalChunks - globalChunk
			var eta string
			if ewmaRate > 0 && remainingChunks > 0 {
				remainingSec := float64(remainingChunks) * float64(f.config.ChunkSize) / ewmaRate
				remaining := time.Duration(remainingSec) * time.Second
				eta = fmt.Sprintf("%s (in ~%s)",
					time.Now().Add(remaining).Format("Jan 2, 3:04pm"),
					remaining.Round(time.Second),
				)
			} else {
				eta = "done"
			}

			log.InfoContext(ctx, "chunk complete",
				"chunk", fmt.Sprintf("%d/%d/%d", c+1, globalChunk, totalChunks),
				"rows", chunkRows,
				"elapsed", elapsed.Round(time.Second),
				"rows_per_sec", int(ewmaRate),
				"eta", eta,
			)
		}
	}

	log.InfoContext(ctx, "fetch complete",
		"tables", len(tables),
		"total_chunks", totalChunks,
		"elapsed", time.Since(start).Round(time.Second),
	)

	return nil
}

func (f *FetchService) ipVersionFilter() string {
	switch f.config.IPVersion {
	case 4:
		return "startsWith(toString(probe_src_addr), '::ffff:')"
	case 6:
		return "NOT startsWith(toString(probe_src_addr), '::ffff:')"
	default:
		return ""
	}
}
