package services

import (
	"context"
	"fmt"

	"github.com/dioptra-io/ufuk-research/internal/schema"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

const (
	defaultFIEChunkSize     = 1_000_000
	defaultFIERTTResolution = 0.1
)

// FIEComputeConfig holds the configuration for the FIE computation service.
type FIEComputeConfig struct {
	ChunkSize     int
	RTTResolution float64
}

// DefaultFIEComputeConfig returns a FIEComputeConfig with sensible defaults.
func DefaultFIEComputeConfig() FIEComputeConfig {
	return FIEComputeConfig{
		ChunkSize:     defaultFIEChunkSize,
		RTTResolution: defaultFIERTTResolution,
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
	// Step 1: Validate source schema.
	sourceSchema, err := f.store.TableSchema(ctx, source)
	if err != nil {
		return fmt.Errorf("fie: failed to get source schema: %w", err)
	}
	if sourceSchema == nil {
		return fmt.Errorf("fie: source table %s.%s does not exist", source.Database, source.Table)
	}
	ok, err := schema.IsSubsetOf(schema.ResultsLiteSchema{}, sourceSchema, false)
	if err != nil {
		return fmt.Errorf("fie: failed to validate source schema: %w", err)
	}
	if !ok {
		return fmt.Errorf("fie: source table %s.%s is missing required columns", source.Database, source.Table)
	}

	// Step 2: Prepare destination table.
	if err := f.store.PrepareTable(ctx, store.PreparationPolicyAppend, dest, schema.FIEsSchema{}); err != nil {
		return fmt.Errorf("fie: failed to prepare destination table: %w", err)
	}

	return nil
}
