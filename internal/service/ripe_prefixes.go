package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/ripe"
	"github.com/dioptra-io/ufuk-research/internal/schema"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

const (
	DefaultRipePrefixesPreparationPolicy = store.PreparationPolicyFail
)

// RipePrefixesConfig holds the configuration for the RipePrefixesService.
type RipePrefixesConfig struct {
	ASNs              []uint32
	PreparationPolicy store.PreparationPolicy
}

// DefaultRipePrefixesConfig returns a RipePrefixesConfig with sensible defaults.
func DefaultRipePrefixesConfig() RipePrefixesConfig {
	return RipePrefixesConfig{
		PreparationPolicy: DefaultRipePrefixesPreparationPolicy,
	}
}

// RipePrefixesService fetches BGP prefix data from the RIPE Stat API
// and stores it into a local ClickHouse table.
type RipePrefixesService struct {
	store      *store.Store
	ripeClient *ripe.RipeClient
	config     RipePrefixesConfig
}

// NewRipePrefixesService creates a new RipePrefixesService with the given store, ripe client and config.
func NewRipePrefixesService(s *store.Store, rc *ripe.RipeClient, cfg RipePrefixesConfig) *RipePrefixesService {
	return &RipePrefixesService{
		store:      s,
		ripeClient: rc,
		config:     cfg,
	}
}

// Fetch fetches prefixes for the configured ASNs at the given date and time of day
// and inserts them into dest. The TimeOfDay is resolved to a UTC timestamp before fetching.
func (s *RipePrefixesService) Fetch(ctx context.Context, dest store.DatabaseTable, date time.Time, tod ripe.TimeOfDay) error {
	t, err := tod.QueryTime(date)
	if err != nil {
		return fmt.Errorf("ripe: failed to resolve time of day: %w", err)
	}
	return s.FetchAt(ctx, dest, t)
}

// FetchAt fetches prefixes for the configured ASNs at the given raw timestamp
// and inserts them into dest.
func (s *RipePrefixesService) FetchAt(ctx context.Context, dest store.DatabaseTable, t time.Time) error {
	log := slog.Default()

	// Step 1: Prepare destination table.
	if err := s.store.PrepareTable(ctx, s.config.PreparationPolicy, dest, schema.RipePrefixesSchema{}); err != nil {
		return fmt.Errorf("ripe: failed to prepare destination table: %w", err)
	}

	// Check if the existing table's schema is equivalent to the target schema.
	targetSchema := schema.RipePrefixesSchema{}
	existingSchema, err := s.store.TableSchema(ctx, dest)
	if err != nil {
		return fmt.Errorf("ripe: failed to get existing table schema: %w", err)
	}
	ok, err := schema.AreEquivalent(targetSchema, existingSchema, false)
	if err != nil {
		return fmt.Errorf("ripe: failed to compare schemas: %w", err)
	}
	if !ok {
		missing, _ := schema.MissingColumns(targetSchema, existingSchema)
		extra, _ := schema.MissingColumns(existingSchema, targetSchema)
		return fmt.Errorf("ripe: destination table %s.%s schema does not match %s, missing columns: %v, extra columns: %v",
			dest.Database, dest.Table, targetSchema.SchemaName(), missing, extra)
	}

	// Step 2: Fetch prefixes from RIPE Stat API.
	log.InfoContext(ctx, "fetching prefixes from RIPE Stat",
		"asns", s.config.ASNs,
		"query_time", t,
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
	)

	prefixes, err := s.ripeClient.PrefixesByASNs(s.config.ASNs).AtTime(t).Fetch()
	if err != nil {
		return fmt.Errorf("ripe: failed to fetch prefixes: %w", err)
	}

	log.InfoContext(ctx, "fetched prefixes", "count", len(prefixes))

	// Step 3: Insert prefixes into ClickHouse using native batch insert.
	fetchedAt := time.Now().UTC()
	qualified := fmt.Sprintf("INSERT INTO %s.%s", dest.Database, dest.Table)

	batch, err := s.store.PrepareBatch(ctx, qualified)
	if err != nil {
		return fmt.Errorf("ripe: failed to prepare batch: %w", err)
	}

	for _, p := range prefixes {
		if err := batch.Append(
			p.ASN,
			p.Network,
			p.PrefixLen,
			p.QueryTime,
			fetchedAt,
		); err != nil {
			return fmt.Errorf("ripe: failed to append row to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("ripe: failed to send batch: %w", err)
	}

	log.InfoContext(ctx, "inserted prefixes",
		"count", len(prefixes),
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
	)

	return nil
}
