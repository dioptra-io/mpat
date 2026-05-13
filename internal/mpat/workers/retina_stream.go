package workers

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	retina "github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/ufuk-research/internal/api"

	_ "github.com/marcboeker/go-duckdb"
)

const defaultBatchSize = 1

const createSQL = `
CREATE TABLE IF NOT EXISTS fies (
    agent_id                VARCHAR NOT NULL,
    probing_directive_id    BIGINT  NOT NULL,
    sequence_number         BIGINT  NOT NULL,
    ip_version              BIGINT  NOT NULL,
    protocol                BIGINT  NOT NULL,
    source_address          VARCHAR NOT NULL,
    destination_address     VARCHAR NOT NULL,
    near_probe_ttl          BIGINT,
    near_reply_address      VARCHAR,
    near_sent_timestamp     VARCHAR,
    near_received_timestamp VARCHAR,
    far_probe_ttl           BIGINT,
    far_reply_address       VARCHAR,
    far_sent_timestamp      VARCHAR,
    far_received_timestamp  VARCHAR,
    production_timestamp    VARCHAR NOT NULL
);
`

const insertSQL = `
INSERT INTO fies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
`

func retinaStream(ctx context.Context, task *api.Task, workerID int, logger *slog.Logger) error {
	if task.RetinaStream == nil {
		return fmt.Errorf("task %s is not a retina stream task", task.UUID)
	}

	req := task.RetinaStream

	if req.Endpoint == "" {
		return fmt.Errorf("retina stream endpoint is required")
	}

	if req.OutputFile == "" {
		return fmt.Errorf("retina stream output file is required")
	}

	duration := time.Duration(req.DurationSeconds) * time.Second
	if duration <= 0 {
		return fmt.Errorf("retina stream duration must be positive")
	}

	if err := os.MkdirAll(filepath.Dir(req.OutputFile), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	streamCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	logger.Info(
		"starting retina stream task",
		"worker_id", workerID,
		"task_uuid", task.UUID,
		"endpoint", req.Endpoint,
		"output_file", req.OutputFile,
		"duration_seconds", req.DurationSeconds,
	)

	db, err := sql.Open("duckdb", req.OutputFile)
	if err != nil {
		return fmt.Errorf("open duckdb database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(streamCtx, createSQL); err != nil {
		return fmt.Errorf("create duckdb schema: %w", err)
	}

	err = streamRetinaToDuckDB(streamCtx, db, req.Endpoint, defaultBatchSize, logger)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Info(
				"retina stream stopped",
				"task_uuid", task.UUID,
				"reason", err,
			)
			return nil
		}

		return err
	}

	logger.Info(
		"retina stream task completed",
		"worker_id", workerID,
		"task_uuid", task.UUID,
		"output_file", req.OutputFile,
	)

	return nil
}

func streamRetinaToDuckDB(
	ctx context.Context,
	db *sql.DB,
	url string,
	batchSize int,
	logger *slog.Logger,
) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("open retina stream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("retina stream returned status %s", resp.Status)
	}

	reader := bufio.NewReader(resp.Body)
	batch := make([]api.SequencedFIE, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := insertBatch(writeCtx, db, batch); err != nil {
			return err
		}

		logger.Debug("flushed retina batch", "rows", len(batch))
		batch = batch[:0]

		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}

			return err
		}

		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {
			var record api.SequencedFIE

			if jsonErr := json.Unmarshal(line, &record); jsonErr == nil {
				batch = append(batch, record)
			} else {
				logger.Debug("skipping invalid json line", "error", jsonErr)
			}

			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				if flushErr := flush(); flushErr != nil {
					return flushErr
				}

				return ctxErr
			}

			return fmt.Errorf("read retina stream: %w", err)
		}
	}

	return flush()
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}

	return fmt.Sprint(v)
}

func insertBatch(ctx context.Context, db *sql.DB, batch []api.SequencedFIE) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, r := range batch {
		near := r.NearInfo
		if near == nil {
			near = &retina.Info{}
		}

		far := r.FarInfo
		if far == nil {
			far = &retina.Info{}
		}

		_, err := stmt.ExecContext(
			ctx,
			r.Agent.AgentID,
			r.ProbingDirectiveID,
			r.SequenceNumber,
			r.IPVersion,
			r.Protocol,
			r.SourceAddress,
			r.DestinationAddress,
			near.ProbeTTL,
			near.ReplyAddress,
			stringValue(near.SentTimestamp),
			stringValue(near.ReceivedTimestamp),
			far.ProbeTTL,
			far.ReplyAddress,
			stringValue(far.SentTimestamp),
			stringValue(far.ReceivedTimestamp),
			stringValue(r.ProductionTimestamp),
		)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}
