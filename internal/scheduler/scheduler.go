// Package scheduler wakes delivery workers when Postgres reports new work.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// channel must match the pg_notify in internal/store/queries/deliveries.sql.
const channel = "forflow_deliveries"

// Reconnect backoff. The floor keeps a brief database restart from adding
// much pickup latency; polling covers the gap either way.
const (
	minBackoff = 500 * time.Millisecond
	maxBackoff = 30 * time.Second
)

// Listen keeps a dedicated connection LISTENing on the deliveries channel and
// calls wake for every notification. It also calls wake after each successful
// LISTEN, because notifications sent while disconnected are lost. Connection
// errors are logged and retried with backoff; Listen returns nil once ctx is
// cancelled.
//
// The connection is not taken from the pool: LISTEN belongs to one session,
// and holding it would permanently shrink the pool.
func Listen(ctx context.Context, connCfg *pgx.ConnConfig, wake func(), log *slog.Logger) error {
	backoff := minBackoff
	for {
		listened, err := session(ctx, connCfg, wake)
		if ctx.Err() != nil {
			return nil
		}
		if listened {
			backoff = minBackoff
		}
		log.Warn("delivery listener disconnected", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// session runs one connection until it fails or ctx is cancelled. listened
// reports whether LISTEN succeeded, so a session that was healthy for a while
// resets the backoff.
func session(ctx context.Context, connCfg *pgx.ConnConfig, wake func()) (listened bool, err error) {
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer func() {
		// ctx may already be cancelled; Close still needs a bounded moment to
		// send Terminate so the server drops the session at once.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return false, fmt.Errorf("listen: %w", err)
	}
	wake()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return true, fmt.Errorf("wait for notification: %w", err)
		}
		wake()
	}
}
