//go:build integration

package delivery_test

// The endpoint health queries live in internal/store/queries/endpoints.sql.
// They are tested here because this package already runs every migration
// against a real Postgres. Each test runs in one transaction, so now() is
// fixed and pauses and ages can be compared exactly.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"forflow/internal/store/sqlc"
)

func iv(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

// healthTx opens a transaction that is rolled back when the test ends.
func healthTx(t *testing.T) (pgx.Tx, *sqlc.Queries) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx, sqlc.New(tx)
}

// txEndpoint inserts an endpoint whose streak started streakAge ago (no
// streak when failures is 0). pausedFor > 0 means the breaker is open for that
// long from now.
func txEndpoint(t *testing.T, tx pgx.Tx, failures int32, streakAge, pausedFor time.Duration) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	err := tx.QueryRow(context.Background(), `
		INSERT INTO endpoints (url, secret, consecutive_failures, first_failure_at, paused_until)
		VALUES ('http://example.invalid/hook', $1, $2,
		        CASE WHEN $2 > 0 THEN now() - $3::interval END,
		        CASE WHEN $4::interval > '0' THEN now() + $4::interval END)
		RETURNING id`, testSecret, failures, iv(streakAge), iv(pausedFor)).Scan(&id)
	if err != nil {
		t.Fatalf("insert endpoint: %v", err)
	}
	return id
}

type txHealth struct {
	Failures  int32
	Streak    bool          // first_failure_at is set
	StreakAge time.Duration // now() - first_failure_at
	Paused    bool          // paused_until is set
	PausedFor time.Duration // paused_until - now()
	Disabled  bool
	// DisabledAge is now() - disabled_at; 0 means disabled by this transaction.
	DisabledAge time.Duration
}

func readTxHealth(t *testing.T, tx pgx.Tx, id pgtype.UUID) txHealth {
	t.Helper()
	var h txHealth
	var streakAge, pausedFor, disabledAge pgtype.Interval
	err := tx.QueryRow(context.Background(), `
		SELECT consecutive_failures,
		       now() - first_failure_at, paused_until - now(), now() - disabled_at
		FROM endpoints WHERE id = $1`, id).Scan(&h.Failures, &streakAge, &pausedFor, &disabledAge)
	if err != nil {
		t.Fatalf("read health: %v", err)
	}
	h.Streak, h.StreakAge = streakAge.Valid, time.Duration(streakAge.Microseconds)*time.Microsecond
	h.Paused, h.PausedFor = pausedFor.Valid, time.Duration(pausedFor.Microseconds)*time.Microsecond
	h.Disabled, h.DisabledAge = disabledAge.Valid, time.Duration(disabledAge.Microseconds)*time.Microsecond
	return h
}

func failureParams(id pgtype.UUID) sqlc.RecordEndpointFailureParams {
	return sqlc.RecordEndpointFailureParams{
		ID:           id,
		Threshold:    3,
		BaseCooldown: iv(30 * time.Second),
		MaxCooldown:  iv(time.Hour),
		DisableAfter: iv(1000 * time.Hour),
	}
}

func TestRecordEndpointFailureCooldown(t *testing.T) {
	tests := []struct {
		name           string
		failuresBefore int32
		wantPause      time.Duration // 0: breaker stays closed
	}{
		{"first failure", 0, 0},
		{"below threshold", 1, 0},
		{"reaches threshold", 2, 30 * time.Second},
		{"one past threshold doubles", 3, time.Minute},
		{"two past threshold doubles again", 4, 2 * time.Minute},
		{"capped at max", 10, time.Hour},
		// Without clamping the exponent, power(2, n) overflows the interval.
		{"huge streak stays capped", 100000, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, q := healthTx(t)
			id := txEndpoint(t, tx, tt.failuresBefore, time.Minute, 0)

			if _, err := q.RecordEndpointFailure(context.Background(), failureParams(id)); err != nil {
				t.Fatalf("record failure: %v", err)
			}
			h := readTxHealth(t, tx, id)
			if h.Failures != tt.failuresBefore+1 {
				t.Errorf("consecutive_failures = %d, want %d", h.Failures, tt.failuresBefore+1)
			}
			if !h.Streak {
				t.Error("first_failure_at is NULL, want the streak start")
			}
			if tt.wantPause == 0 {
				if h.Paused {
					t.Errorf("paused for %v, want breaker closed", h.PausedFor)
				}
				return
			}
			if !h.Paused || h.PausedFor != tt.wantPause {
				t.Errorf("paused for %v (set=%v), want %v", h.PausedFor, h.Paused, tt.wantPause)
			}
		})
	}
}

func TestRecordEndpointFailureStartsStreak(t *testing.T) {
	tx, q := healthTx(t)
	id := txEndpoint(t, tx, 0, 0, 0)

	if _, err := q.RecordEndpointFailure(context.Background(), failureParams(id)); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if h := readTxHealth(t, tx, id); !h.Streak || h.StreakAge != 0 {
		t.Errorf("streak age = %v (set=%v), want started now", h.StreakAge, h.Streak)
	}

	// The second failure must keep the original start, or the streak never ages.
	exec := func(sql string) {
		if _, err := tx.Exec(context.Background(), sql, id); err != nil {
			t.Fatalf("exec: %v", err)
		}
	}
	exec(`UPDATE endpoints SET first_failure_at = now() - interval '10 minutes' WHERE id = $1`)
	if _, err := q.RecordEndpointFailure(context.Background(), failureParams(id)); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if h := readTxHealth(t, tx, id); h.StreakAge != 10*time.Minute {
		t.Errorf("streak age = %v, want 10m (start kept)", h.StreakAge)
	}
}

// Attempts claimed before the breaker opened fail while it is open; they must
// not keep pushing the pause out.
func TestRecordEndpointFailureKeepsOpenPause(t *testing.T) {
	tx, q := healthTx(t)
	id := txEndpoint(t, tx, 5, time.Minute, 10*time.Minute)

	if _, err := q.RecordEndpointFailure(context.Background(), failureParams(id)); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if h := readTxHealth(t, tx, id); h.PausedFor != 10*time.Minute {
		t.Errorf("paused for %v, want the existing 10m", h.PausedFor)
	}
}

func TestRecordEndpointFailureDisables(t *testing.T) {
	tests := []struct {
		name           string
		failuresBefore int32
		streakAge      time.Duration
		disableAfter   time.Duration
		wantDisabled   bool
	}{
		// There is no streak to age yet, even with disable_after 0.
		{"first failure never disables", 0, 0, 0, false},
		{"young streak", 1, 30 * time.Minute, time.Hour, false},
		{"streak exactly at limit", 1, time.Hour, time.Hour, true},
		{"old streak", 1, 2 * time.Hour, time.Hour, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, q := healthTx(t)
			id := txEndpoint(t, tx, tt.failuresBefore, tt.streakAge, 0)
			p := failureParams(id)
			p.Threshold = 100 // keep the breaker out of it
			p.DisableAfter = iv(tt.disableAfter)

			disabledNow, err := q.RecordEndpointFailure(context.Background(), p)
			if err != nil {
				t.Fatalf("record failure: %v", err)
			}
			if disabledNow != tt.wantDisabled {
				t.Errorf("disabled_now = %v, want %v", disabledNow, tt.wantDisabled)
			}
			if h := readTxHealth(t, tx, id); h.Disabled != tt.wantDisabled {
				t.Errorf("disabled = %v, want %v", h.Disabled, tt.wantDisabled)
			}
		})
	}
}

// disabled_now reports the transition only, so the worker logs it once.
func TestRecordEndpointFailureReportsDisableOnce(t *testing.T) {
	tx, q := healthTx(t)
	id := txEndpoint(t, tx, 1, 2*time.Hour, 0)
	if _, err := tx.Exec(context.Background(),
		`UPDATE endpoints SET disabled_at = now() - interval '5 minutes' WHERE id = $1`, id); err != nil {
		t.Fatalf("disable: %v", err)
	}
	p := failureParams(id)
	p.DisableAfter = iv(time.Hour)

	disabledNow, err := q.RecordEndpointFailure(context.Background(), p)
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if disabledNow {
		t.Error("disabled_now = true for an endpoint disabled earlier, want false")
	}
	if h := readTxHealth(t, tx, id); h.DisabledAge != 5*time.Minute {
		t.Errorf("disabled %v ago, want the original 5m", h.DisabledAge)
	}
}

func TestRecordEndpointSuccess(t *testing.T) {
	tx, q := healthTx(t)
	id := txEndpoint(t, tx, 4, 10*time.Minute, 5*time.Minute)
	if _, err := tx.Exec(context.Background(),
		`UPDATE endpoints SET disabled_at = now() WHERE id = $1`, id); err != nil {
		t.Fatalf("disable: %v", err)
	}

	if err := q.RecordEndpointSuccess(context.Background(), id); err != nil {
		t.Fatalf("record success: %v", err)
	}
	h := readTxHealth(t, tx, id)
	if h.Failures != 0 || h.Streak || h.Paused {
		t.Errorf("health = %+v, want streak and pause cleared", h)
	}
	// Re-enabling stays a human decision, even after a late success.
	if !h.Disabled {
		t.Error("disabled_at cleared by a success, want it kept")
	}
}

func TestEnableEndpointClearsHealth(t *testing.T) {
	tx, q := healthTx(t)
	id := txEndpoint(t, tx, 7, 3*time.Hour, 20*time.Minute)
	if _, err := tx.Exec(context.Background(),
		`UPDATE endpoints SET disabled_at = now() WHERE id = $1`, id); err != nil {
		t.Fatalf("disable: %v", err)
	}

	n, err := q.EnableEndpoint(context.Background(), id)
	if err != nil || n != 1 {
		t.Fatalf("enable: n=%d err=%v", n, err)
	}
	// A kept first_failure_at would disable the endpoint again on its next failure.
	if h := readTxHealth(t, tx, id); h.Disabled || h.Failures != 0 || h.Streak || h.Paused {
		t.Errorf("health = %+v, want everything cleared", h)
	}

	var unknown pgtype.UUID
	if err := unknown.Scan("01a1a58e-0000-7000-8000-000000000000"); err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	if n, err := q.EnableEndpoint(context.Background(), unknown); err != nil || n != 0 {
		t.Errorf("enable unknown endpoint: n=%d err=%v, want 0 rows", n, err)
	}
}
