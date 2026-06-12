package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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

//go:embed templates/fie_resultslite_insert.tmpl
var fieInsertResultsLiteTemplate string

//go:embed templates/fie_resultslite_cursor.tmpl
var fieCursorResultsLiteTemplate string

// CardinalityPolicy controls how many unique reply addresses are allowed
// on each side of a (near, far) hop pair.
type CardinalityPolicy string

const (
	// CardinalityOneToOne keeps pairs where both near and far have exactly
	// one unique reply address (far may also be absent; nullity decides).
	CardinalityOneToOne CardinalityPolicy = "one_to_one"
	// CardinalityManyToOne keeps pairs where far has exactly one unique
	// reply address (far may also be absent; nullity decides).
	CardinalityManyToOne CardinalityPolicy = "many_to_one"
	// CardinalityOneToMany keeps pairs where near has exactly one unique
	// reply address.
	CardinalityOneToMany CardinalityPolicy = "one_to_many"
	// CardinalityAll keeps all pairs (full cartesian product on explode).
	CardinalityAll CardinalityPolicy = "all"
)

// Condition returns the ClickHouse WHERE fragment implementing the policy.
//
// The conditions are "tolerant": they constrain a side only when it is
// non-empty, so that the nullity policy alone decides whether rows with a
// missing far hop survive.
func (p CardinalityPolicy) Condition() (string, error) {
	switch p {
	case CardinalityOneToOne:
		return "length(near.reply_addrs) = 1 AND length(far.reply_addrs) <= 1", nil
	case CardinalityManyToOne:
		return "length(far.reply_addrs) <= 1", nil
	case CardinalityOneToMany:
		return "length(near.reply_addrs) = 1", nil
	case CardinalityAll:
		return "1 = 1", nil
	default:
		return "", fmt.Errorf("fie: unknown cardinality policy %q", p)
	}
}

// NullityPolicy controls which combinations of present/absent replies are
// allowed in a (near, far) hop pair. The near side is always present by
// construction (the LEFT JOIN is anchored on near), so only the far side
// is constrained.
type NullityPolicy string

const (
	// NullityBothSome keeps only pairs where the far hop has at least one
	// reply address.
	NullityBothSome NullityPolicy = "both_some"
	// NullityFarNone keeps only pairs where the far hop has no reply
	// addresses (missing h+1 hop, or hop with no replies).
	NullityFarNone NullityPolicy = "far_none"
	// NullityAny keeps all pairs regardless of the far side.
	NullityAny NullityPolicy = "any"
)

// Condition returns the ClickHouse WHERE fragment implementing the policy.
func (p NullityPolicy) Condition() (string, error) {
	switch p {
	case NullityBothSome:
		return "length(far.reply_addrs) > 0", nil
	case NullityFarNone:
		return "length(far.reply_addrs) = 0", nil
	case NullityAny:
		return "1 = 1", nil
	default:
		return "", fmt.Errorf("fie: unknown nullity policy %q", p)
	}
}

// ValidatePolicies rejects policy combinations that are contradictory or
// degenerate.
func ValidatePolicies(c CardinalityPolicy, n NullityPolicy) error {
	if _, err := c.Condition(); err != nil {
		return err
	}
	if _, err := n.Condition(); err != nil {
		return err
	}
	// When far is required to be empty, constraining far's cardinality is
	// meaningless: one_to_one degenerates to one_to_many, and many_to_one
	// degenerates to all.
	if n == NullityFarNone && (c == CardinalityOneToOne || c == CardinalityManyToOne) {
		return fmt.Errorf("fie: cardinality policy %q is meaningless with nullity policy %q (far is always empty)", c, n)
	}
	return nil
}

type fieTemplateData struct {
	SourceDatabase       string
	SourceTable          string
	DestDatabase         string
	DestTable            string
	ChunkSize            int
	RTTResolution        float64
	Cursor               string
	NullityCondition     string
	CardinalityCondition string
}

// FIEComputeConfig holds the configuration for the FIE computation service.
type FIEComputeConfig struct {
	ChunkSize         int
	RTTResolution     float64
	PreparationPolicy store.PreparationPolicy
	Cardinality       CardinalityPolicy
	Nullity           NullityPolicy
}

// DefaultFIEComputeConfig returns a FIEComputeConfig with sensible defaults.
//
// The default policies (one_to_one, both_some) reproduce the behavior of the
// original query, which kept only pairs where both hops had exactly one
// unique reply address.
func DefaultFIEComputeConfig() FIEComputeConfig {
	return FIEComputeConfig{
		ChunkSize:     DefaultFIEChunkSize,
		RTTResolution: DefaultFIERTTResolution,
		Cardinality:   CardinalityOneToOne,
		Nullity:       NullityBothSome,
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
	log := slog.Default()

	// Step 0: Validate the filtering policy combination.
	if err := ValidatePolicies(f.config.Cardinality, f.config.Nullity); err != nil {
		return err
	}

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

	log.InfoContext(ctx, "detected source schema",
		"schema", detectedSchema.SchemaName(),
		"source", fmt.Sprintf("%s.%s", source.Database, source.Table),
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
		"cardinality_policy", string(f.config.Cardinality),
		"nullity_policy", string(f.config.Nullity),
	)

	// Step 2: Prepare destination table.
	if err := f.store.PrepareTable(ctx, f.config.PreparationPolicy, dest, schema.FIEsSchema{}); err != nil {
		return fmt.Errorf("fie: failed to prepare destination table: %w", err)
	}

	// Step 3: Run the keyset-paginated INSERT loop.
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

		log.InfoContext(ctx, "chunk complete",
			"chunk", chunk,
			"cursor", cursor,
			"last", lastPrefix,
			"rows_inserted", rowsInserted,
			"total_rows", totalRows,
			"elapsed", time.Since(chunkStart).Round(time.Millisecond),
		)

		cursor = lastPrefix
	}

	log.InfoContext(ctx, "compute complete",
		"chunks", chunk,
		"total_rows", totalRows,
		"cardinality_policy", string(f.config.Cardinality),
		"nullity_policy", string(f.config.Nullity),
		"elapsed", time.Since(start).Round(time.Second),
	)

	return nil
}

func (f *FIEComputeService) fiesLastPrefix(ctx context.Context, source store.DatabaseTable, cursor string, s schema.Schema) (string, error) {
	var tmpl string
	switch s.(type) {
	case schema.ResultsSchema, schema.ResultsLiteSchema:
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
	case schema.ResultsSchema, schema.ResultsLiteSchema:
		tmpl = fieInsertResultsLiteTemplate
	default:
		return fmt.Errorf("fie: unsupported source schema %s", s.SchemaName())
	}

	nullityCond, err := f.config.Nullity.Condition()
	if err != nil {
		return err
	}
	cardinalityCond, err := f.config.Cardinality.Condition()
	if err != nil {
		return err
	}

	query, err := renderTemplate("fie_insert", tmpl, fieTemplateData{
		SourceDatabase:       source.Database,
		SourceTable:          source.Table,
		DestDatabase:         dest.Database,
		DestTable:            dest.Table,
		ChunkSize:            f.config.ChunkSize,
		RTTResolution:        f.config.RTTResolution,
		Cursor:               cursor,
		NullityCondition:     nullityCond,
		CardinalityCondition: cardinalityCond,
	})
	if err != nil {
		return fmt.Errorf("fie: failed to render insert template: %w", err)
	}
	if err := f.store.Exec(ctx, query); err != nil {
		return fmt.Errorf("fie: failed to execute insert: %w", err)
	}
	return nil
}
