//go:build integration

package scheduler_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"forflow/internal/scheduler"
	"forflow/internal/store/sqlc"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run needs no migrations: LISTEN/NOTIFY uses no tables.
func run(m *testing.M) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("forflow"),
		postgres.WithUsername("forflow"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("open pool: %v", err)
		return 1
	}
	defer pool.Close()

	return m.Run()
}

// startListener runs Listen until the test ends. Every wake lands on the
// returned channel; stop cancels Listen and returns its error.
func startListener(t *testing.T) (wakes <-chan struct{}, stop func() error) {
	t.Helper()
	ch := make(chan struct{}, 100)
	wake := func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { done <- scheduler.Listen(ctx, pool.Config().ConnConfig, wake, logger) }()

	stopped := false
	var runErr error
	stop = func() error {
		if stopped {
			return runErr
		}
		stopped = true
		cancel()
		select {
		case runErr = <-done:
		case <-time.After(5 * time.Second):
			runErr = fmt.Errorf("Listen did not return within 5s of cancel")
		}
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return ch, stop
}

func waitWake(t *testing.T, wakes <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-wakes:
	case <-time.After(within):
		t.Fatalf("no wake within %v: %s", within, what)
	}
}

// listenerPIDs returns the backends currently LISTENing, as seen by Postgres.
func listenerPIDs(t *testing.T) []int32 {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT pid FROM pg_stat_activity WHERE query = 'LISTEN forflow_deliveries'`)
	if err != nil {
		t.Fatalf("query listeners: %v", err)
	}
	var pids []int32
	for rows.Next() {
		var pid int32
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan pid: %v", err)
		}
		pids = append(pids, pid)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("listeners: %v", err)
	}
	return pids
}

// waitListeners polls until exactly want backends are LISTENing.
func waitListeners(t *testing.T, want int) []int32 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		pids := listenerPIDs(t)
		if len(pids) == want {
			return pids
		}
		if time.Now().After(deadline) {
			t.Fatalf("listening backends = %v, want %d", pids, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func notify(t *testing.T) {
	t.Helper()
	// The real query, so a channel name mismatch with Listen fails here.
	if err := sqlc.New(pool).NotifyDeliveries(context.Background()); err != nil {
		t.Fatalf("notify: %v", err)
	}
}

// Notifications sent while disconnected are lost, so a fresh LISTEN must
// wake the worker once even when nothing was sent.
func TestListenerWakesAfterConnect(t *testing.T) {
	wakes, _ := startListener(t)
	waitWake(t, wakes, 2*time.Second, "after connect")
}

func TestListenerWakesOnNotify(t *testing.T) {
	wakes, _ := startListener(t)
	waitWake(t, wakes, 2*time.Second, "after connect")

	notify(t)
	waitWake(t, wakes, time.Second, "after notify")
}

func TestListenerReconnects(t *testing.T) {
	wakes, _ := startListener(t)
	waitWake(t, wakes, 2*time.Second, "after connect")
	pids := waitListeners(t, 1)

	var terminated bool
	if err := pool.QueryRow(context.Background(),
		`SELECT pg_terminate_backend($1)`, pids[0]).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate listener backend %d: %v (terminated=%v)", pids[0], err, terminated)
	}
	waitWake(t, wakes, 5*time.Second, "after reconnect")

	// LISTEN is per session, so the new connection must have issued it again.
	notify(t)
	waitWake(t, wakes, time.Second, "after notify on the new connection")
}

// Invariant 6: shutdown is bounded and leaves no session behind.
func TestListenerStopsOnCancel(t *testing.T) {
	wakes, stop := startListener(t)
	waitWake(t, wakes, 2*time.Second, "after connect")
	waitListeners(t, 1)

	start := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("Listen returned %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Listen took %v to return after cancel, want <= 1s", elapsed)
	}
	waitListeners(t, 0)
}
