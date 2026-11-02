package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/store/sqlc"
)

// retrySchedule is the wait before each retry, following Svix. The first
// attempt runs as soon as the delivery is due, so a delivery gets
// len(retrySchedule)+1 attempts in total (about 1.3 days end to end).
var retrySchedule = []time.Duration{
	5 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	5 * time.Hour,
	10 * time.Hour,
	10 * time.Hour,
}

// maxDrainBytes bounds how much of a response body is read, so an endpoint
// cannot stream unbounded data into the worker.
const maxDrainBytes = 64 << 10

// Worker claims due deliveries and sends them to customer endpoints.
type Worker struct {
	q          *sqlc.Queries
	cfg        Config
	lease      pgtype.Interval
	httpClient *http.Client
	log        *slog.Logger
	// now and jitter are injected so tests can pin retry times.
	now    func() time.Time
	jitter func(time.Duration) time.Duration
}

// Config holds the worker's timing knobs. Lease must exceed HTTPTimeout, or
// slow but healthy attempts are claimed again and delivered twice.
type Config struct {
	HTTPTimeout  time.Duration
	Lease        time.Duration
	PollInterval time.Duration
	Logger       *slog.Logger
}

// New returns a Worker that uses pool for claims and outcomes.
func New(pool *pgxpool.Pool, cfg Config) *Worker {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		q:   sqlc.New(pool),
		cfg: cfg,
		lease: pgtype.Interval{
			Microseconds: cfg.Lease.Microseconds(),
			Valid:        true,
		},
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
			// A 3xx is a failed attempt
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log:    log,
		now:    time.Now,
		jitter: tenPercentJitter,
	}
}

// Run claims and delivers until ctx is cancelled. An attempt already in
// flight when ctx is cancelled runs to completion before Run returns.
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
		if ctx.Err() != nil {
			w.release(workCtx, job)
			return nil
		}

		res := w.send(workCtx, job)
		w.record(workCtx, job, res)
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

type outcome int

const (
	outcomeSent           outcome = iota // endpoint replied 2xx
	outcomeEndpointFailed                // endpoint was contacted but did not accept
	outcomeDiscarded                     // we failed before contacting the endpoint
)

type sendResult struct {
	outcome        outcome
	responseStatus *int32 // nil when no HTTP response was received
	errorKind      string // short fixed label; never err.Error(), which may carry customer URLs
	duration       time.Duration
}

func (w *Worker) send(ctx context.Context, job sqlc.ClaimDeliveryRow) sendResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.Endpoint, bytes.NewReader(job.Payload))
	if err != nil {
		return sendResult{outcome: outcomeDiscarded, errorKind: "invalid_request"}
	}
	req.Header.Set("Content-Type", "application/json")

	start := w.now()
	resp, err := w.httpClient.Do(req)
	duration := w.now().Sub(start)
	if err != nil {
		return sendResult{outcome: outcomeEndpointFailed, errorKind: errorKind(err), duration: duration}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))

	code := int32(resp.StatusCode)
	result := sendResult{outcome: outcomeEndpointFailed, responseStatus: &code, duration: duration}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result.outcome = outcomeSent
	}
	return result
}

// errorKind maps a transport error to a fixed label. DNS is checked first
// because *net.DNSError also reports Timeout().
func errorKind(err error) string {
	var dnsErr *net.DNSError
	var netErr net.Error
	switch {
	case errors.As(err, &dnsErr):
		return "dns"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "conn_refused"
	}
	return "other"
}

// nextRetry returns the wait before the next attempt, given how many attempts
// were made before the one that just failed. ok is false once the schedule is
// exhausted.
func nextRetry(attemptsBefore int32) (wait time.Duration, ok bool) {
	if attemptsBefore < 0 || int(attemptsBefore) >= len(retrySchedule) {
		return 0, false
	}
	return retrySchedule[attemptsBefore], true
}

// tenPercentJitter spreads retries of endpoints that failed together, so they
// do not all come back in the same second.
func tenPercentJitter(d time.Duration) time.Duration {
	if d < 10 {
		return d
	}
	return d + rand.N(d/10)
}

// record stores the attempt's outcome. Errors are logged, not returned: a lost
// lease means another worker owns the row, and a database error leaves the
// lease to expire so the row is claimed again without spending an attempt.
func (w *Worker) record(ctx context.Context, job sqlc.ClaimDeliveryRow, res sendResult) {
	var code pgtype.Int4
	if res.responseStatus != nil {
		code = pgtype.Int4{Int32: *res.responseStatus, Valid: true}
	}

	var (
		rows   int64
		err    error
		status string
	)
	switch res.outcome {
	case outcomeSent:
		status = "sent"
		rows, err = w.q.MarkSent(ctx, sqlc.MarkSentParams{ID: job.ID, LeaseID: job.LeaseID, ResponseStatus: code})
	case outcomeDiscarded:
		status = "discarded"
		rows, err = w.q.MarkDiscarded(ctx, sqlc.MarkDiscardedParams{ID: job.ID, LeaseID: job.LeaseID})
	case outcomeEndpointFailed:
		if wait, ok := nextRetry(job.AttemptCount); ok {
			status = "retry"
			next := w.now().Add(w.jitter(wait)).UTC()
			rows, err = w.q.ScheduleRetry(ctx, sqlc.ScheduleRetryParams{
				ID:             job.ID,
				LeaseID:        job.LeaseID,
				ResponseStatus: code,
				NextAttemptAt:  pgtype.Timestamptz{Time: next, Valid: true},
			})
		} else {
			status = "failed"
			rows, err = w.q.MarkFailed(ctx, sqlc.MarkFailedParams{ID: job.ID, LeaseID: job.LeaseID, ResponseStatus: code})
		}
	}

	attrs := []any{
		"delivery_id", job.ID.String(),
		"attempt", job.AttemptCount + 1,
		"outcome", status,
		"error_kind", res.errorKind,
		"duration_ms", res.duration.Milliseconds(),
	}
	if res.responseStatus != nil {
		attrs = append(attrs, "response_status", *res.responseStatus)
	}
	switch {
	case err != nil:
		w.log.Error("record delivery outcome", append(attrs, "error", err)...)
	case rows == 0:
		// Not a bug: the attempt outlived its lease and another worker
		// reclaimed the row. That worker sends again and records its own result.
		w.log.Warn("lease lost before recording outcome", attrs...)
	default:
		w.log.Info("delivery attempt recorded", attrs...)
	}
}

func (w *Worker) release(ctx context.Context, job sqlc.ClaimDeliveryRow) {
	if _, err := w.q.ReleaseLease(ctx, sqlc.ReleaseLeaseParams{ID: job.ID, LeaseID: job.LeaseID}); err != nil {
		w.log.Error("release lease", "delivery_id", job.ID.String(), "error", err)
	}
}
