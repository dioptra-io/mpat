package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/retina"
	"github.com/dioptra-io/ufuk-research/internal/schema"
	"github.com/dioptra-io/ufuk-research/internal/store"
)

const (
	DefaultRetinaPreparationPolicy = store.PreparationPolicyFail
)

// RetinaConfig holds the configuration for the RetinaService.
type RetinaConfig struct {
	PreparationPolicy store.PreparationPolicy
}

// DefaultRetinaConfig returns a RetinaConfig with sensible defaults.
func DefaultRetinaConfig() RetinaConfig {
	return RetinaConfig{
		PreparationPolicy: DefaultRetinaPreparationPolicy,
	}
}

// RetinaService streams FIEs from the Retina API and inserts them into
// a local ClickHouse table.
type RetinaService struct {
	store        *store.Store
	retinaClient *retina.RetinaClient
	config       RetinaConfig
}

// NewRetinaService creates a new RetinaService with the given store, retina client and config.
func NewRetinaService(s *store.Store, rc *retina.RetinaClient, cfg RetinaConfig) *RetinaService {
	return &RetinaService{
		store:        s,
		retinaClient: rc,
		config:       cfg,
	}
}

// Stream streams FIEs from the Retina API and inserts them into dest.
func (s *RetinaService) Stream(ctx context.Context, dest store.DatabaseTable) error {
	log := slog.Default()

	// Step 1: Prepare destination table.
	if err := s.store.PrepareTable(ctx, s.config.PreparationPolicy, dest, schema.FIEsSchema{}); err != nil {
		return fmt.Errorf("retina: failed to prepare destination table: %w", err)
	}

	// Step 2: Validate schema.
	targetSchema := schema.FIEsSchema{}
	existingSchema, err := s.store.TableSchema(ctx, dest)
	if err != nil {
		return fmt.Errorf("retina: failed to get existing table schema: %w", err)
	}
	ok, err := schema.AreEquivalent(targetSchema, existingSchema, false)
	if err != nil {
		return fmt.Errorf("retina: failed to compare schemas: %w", err)
	}
	if !ok {
		missing, _ := schema.MissingColumns(targetSchema, existingSchema)
		extra, _ := schema.MissingColumns(existingSchema, targetSchema)
		return fmt.Errorf("retina: destination table %s.%s schema does not match %s, missing columns: %v, extra columns: %v",
			dest.Database, dest.Table, targetSchema.SchemaName(), missing, extra)
	}

	// Step 3: Stream and insert.
	log.InfoContext(ctx, "streaming FIEs from Retina",
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
	)
	var total int
	for r := range s.retinaClient.Stream(ctx) {
		if r.Err != nil {
			if errors.Is(r.Err, context.DeadlineExceeded) || errors.Is(r.Err, context.Canceled) {
				break
			}
			return fmt.Errorf("retina: stream error: %w", r.Err)
		}
		if err := s.insertBatch(context.Background(), dest, r.Batch); err != nil {
			return err
		}
		total += len(r.Batch)
		log.InfoContext(ctx, "inserted batch",
			"count", len(r.Batch),
			"total", total,
			"last_sequence_number", r.Batch[len(r.Batch)-1].SequenceNumber,
		)
	}
	// Step 4: Log completion.
	log.InfoContext(ctx, "stream complete",
		"total", total,
		"dest", fmt.Sprintf("%s.%s", dest.Database, dest.Table),
	)
	return nil
}

// insertBatch inserts a batch of SequencedFIEs into dest.
func (s *RetinaService) insertBatch(ctx context.Context, dest store.DatabaseTable, batch []retina.SequencedFIE) error {
	b, err := s.store.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dest.Database, dest.Table))
	if err != nil {
		return fmt.Errorf("retina: failed to prepare batch: %w", err)
	}

	zeroIP := net.IPv6zero
	zeroTime := time.Time{}

	for _, fie := range batch {
		agentID := net.ParseIP(fie.Agent.AgentID)
		if agentID == nil {
			agentID = zeroIP
		}

		nearTTL := uint8(0)
		nearReply := zeroIP
		nearSent := zeroTime
		nearReceived := zeroTime
		if fie.NearInfo != nil {
			nearTTL = fie.NearInfo.ProbeTTL
			nearReply = fie.NearInfo.ReplyAddress
			nearSent = fie.NearInfo.SentTimestamp
			nearReceived = fie.NearInfo.ReceivedTimestamp
		}

		farTTL := uint8(0)
		farReply := zeroIP
		farSent := zeroTime
		farReceived := zeroTime
		if fie.FarInfo != nil {
			farTTL = fie.FarInfo.ProbeTTL
			farReply = fie.FarInfo.ReplyAddress
			farSent = fie.FarInfo.SentTimestamp
			farReceived = fie.FarInfo.ReceivedTimestamp
		}

		if err := b.Append( // this should be the same as the column names.
			fie.SequenceNumber,
			agentID,
			fie.ProbingDirectiveID,
			uint8(fie.IPVersion),
			uint8(fie.Protocol),
			fie.SourceAddress,
			fie.DestinationAddress,
			nearTTL,
			nearReply,
			nearSent,
			nearReceived,
			farTTL,
			farReply,
			farSent,
			farReceived,
			fie.ProductionTimestamp,
		); err != nil {
			return fmt.Errorf("retina: failed to append row (seq=%d): %w", fie.SequenceNumber, err)
		}
	}

	if err := b.Send(); err != nil {
		return fmt.Errorf("retina: failed to send batch: %w", err)
	}

	return nil
}
