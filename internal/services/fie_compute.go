package services

import (
	"context"

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
func NewFIEComputeService(s *store.Store, cfg FIEComputeConfig) *FIEComputeService {
	return &FIEComputeService{
		store:  s,
		config: cfg,
	}
}

// Compute computes FIEs from source into dest.
func (f *FIEComputeService) Compute(ctx context.Context, source, dest store.DatabaseTable) error {
	// TODO: implement
	return nil
}
