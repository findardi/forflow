//go:build integration

package delivery_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"forflow/internal/delivery"
	"forflow/internal/message"
	"forflow/internal/scheduler"
)

// wakeConfig polls so rarely that anything delivered within a second was
// picked up by a wakeup, not by the poll.
func wakeConfig(workers int, maxInFlight int32) delivery.Config {
	cfg := hangConfig(workers, maxInFlight)
	cfg.PollInterval = 30 * time.Second
	return cfg
}

// settle gives every loop time to run its first empty claim and fall asleep,
// so a later delivery can only be picked up by a wakeup.
func settle() { time.Sleep(300 * time.Millisecond) }

func TestWakeDeliversBeforePoll(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)

	cfg := wakeConfig(3, 1)
	w := delivery.New(pool, cfg)
	stop := runWorker(t, w)
	settle()

	id := seed(t, srv.URL, []byte(`{}`))
	start := time.Now()
	w.Wake()
	waitStatus(t, id, "sent")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("sent %v after Wake, want < 2s (PollInterval is %v)", elapsed, cfg.PollInterval)
	}

	// Invariant 6: a loop asleep on wake/poll still stops at once.
	elapsed, err := stop()
	if err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if elapsed > time.Second {
		t.Errorf("Run took %v to return after cancel, want <= 1s", elapsed)
	}
}

// One Wake must reach every idle loop when there is enough work: each loop
// that claims wakes the next. Without the chain one loop takes a delivery and
// the rest sleep until PollInterval.
func TestWakeChainsAcrossLoops(t *testing.T) {
	reset(t)
	w := delivery.New(pool, wakeConfig(4, 4))
	runWorker(t, w)
	settle()

	hang := newHangingEndpoint(t, 4)
	// Registered after newHangingEndpoint, so it runs before srv.Close.
	t.Cleanup(hang.unblock)
	w.Wake()

	deadline := time.Now().Add(time.Second)
	for {
		if _, peak := hang.inFlight(); peak == 4 {
			return
		}
		if time.Now().After(deadline) {
			_, peak := hang.inFlight()
			t.Fatalf("peak in-flight = %d one second after a single Wake, want 4", peak)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Wake is called from the listener; if it blocked, one busy moment would
// stall every later notification.
func TestWakeNeverBlocks(t *testing.T) {
	w := delivery.New(pool, testConfig) // never Run, so nobody drains wakeups
	done := make(chan struct{})
	go func() {
		for range 1000 {
			w.Wake()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wake blocked with no loop waiting")
	}
}

// End to end: ingest commits, Postgres notifies, the listener wakes the
// worker, and the delivery goes out long before the poll would find it.
func TestIngestWakesWorker(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	seedEndpoint(t, srv.URL)

	w := delivery.New(pool, wakeConfig(2, 1))
	runWorker(t, w)

	ctx, cancel := context.WithCancel(context.Background())
	listened := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { listened <- scheduler.Listen(ctx, pool.Config().ConnConfig, w.Wake, logger) }()
	t.Cleanup(func() {
		cancel()
		if err := <-listened; err != nil {
			t.Errorf("Listen returned %v, want nil", err)
		}
	})
	// The listener's connect wake is spent on an empty claim before ingest.
	settle()

	msg, n, err := message.NewService(pool).Create(context.Background(), []byte(`{"order":1}`))
	if err != nil || n != 1 {
		t.Fatalf("create: n=%d err=%v", n, err)
	}
	start := time.Now()
	var id string
	if err := pool.QueryRow(context.Background(),
		`SELECT id::text FROM deliveries WHERE message_id = $1`, msg.ID).Scan(&id); err != nil {
		t.Fatalf("find delivery: %v", err)
	}
	waitStatus(t, id, "sent")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("sent %v after ingest, want < 2s", elapsed)
	}
}
