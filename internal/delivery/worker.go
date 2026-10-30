package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/store/sqlc"
)

type Worker struct {
	q          *sqlc.Queries
	cfg        Config
	lease      pgtype.Interval
	httpClient *http.Client
}

type Config struct {
	HTTPTimeout  time.Duration
	Lease        time.Duration
	PollInterval time.Duration
}

func New(pool *pgxpool.Pool, cfg Config) *Worker {
	return &Worker{
		q:   sqlc.New(pool),
		cfg: cfg,
		lease: pgtype.Interval{
			Microseconds: cfg.Lease.Microseconds(),
			Valid:        true,
		},
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		job, ok, err := w.claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if !ok {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(w.cfg.PollInterval):
			}
			continue
		}

		workCtx := context.WithoutCancel(ctx)
		res := w.send(workCtx, job)
		if err := w.record(workCtx, job, res); err != nil {
			return err
		}
	}
}

func (w *Worker) claim(ctx context.Context) (sqlc.ClaimDeliveryRow, bool, error) {
	job, err := w.q.ClaimDelivery(ctx, w.lease)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, false, nil
	}

	if err != nil {
		return sqlc.ClaimDeliveryRow{}, false, fmt.Errorf("get claim delivery: %w", err)
	}

	return job, true, nil
}

type sendResult struct {
	status         string
	responseStatus *int32
}

func (w *Worker) send(ctx context.Context, job sqlc.ClaimDeliveryRow) sendResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.Endpoint, bytes.NewReader(job.Payload))
	if err != nil {
		return sendResult{
			status: "failed",
		}
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return sendResult{
			status: "failed",
		}
	}

	defer resp.Body.Close()

	code := int32(resp.StatusCode)
	result := sendResult{
		status:         "failed",
		responseStatus: &code,
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result.status = "sent"
	}
	return result
}

func (w *Worker) record(ctx context.Context, job sqlc.ClaimDeliveryRow, result sendResult) error {
	var code pgtype.Int4
	if result.responseStatus != nil {
		code = pgtype.Int4{Int32: *result.responseStatus, Valid: true}
	}
	if err := w.q.RecordDelivery(ctx, sqlc.RecordDeliveryParams{
		Status:         result.status,
		ResponseStatus: code,
		ID:             job.ID,
	}); err != nil {
		return fmt.Errorf("record delivery: %w", err)
	}

	return nil
}
