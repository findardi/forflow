//go:build integration

package delivery_test

// These tests are the spec for the circuit breaker and auto-disable. The
// implementation is owner-written (red zone: claim.sql and Worker.record);
// the tests only fix the public surface:
//
//	delivery.Config{
//		BreakerThreshold   int32         // consecutive failures that open the breaker
//		BreakerCooldown    time.Duration // first pause; doubles per further failure
//		BreakerMaxCooldown time.Duration // ceiling for the pause
//		DisableAfter       time.Duration // failing streak age that disables the endpoint
//	}
//
// and the log line "endpoint disabled" (with endpoint_id) when a failure
// disables an endpoint.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"forflow/internal/delivery"
)

// countingServer answers every request with status and counts them.
func countingServer(t *testing.T, status int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// breakerConfig runs one loop, so claims and records happen strictly in order.
func breakerConfig(threshold int32) delivery.Config {
	cfg := testConfig
	cfg.BreakerThreshold = threshold
	cfg.BreakerCooldown = time.Hour
	cfg.BreakerMaxCooldown = 24 * time.Hour
	return cfg
}

// setHealth puts an endpoint mid-streak. pausedFor < 0 means the pause has
// already ended (half-open once failures reach the threshold).
func setHealth(t *testing.T, endpointID string, failures int32, streakAge, pausedFor time.Duration) {
	t.Helper()
	exec(t, `
		UPDATE endpoints
		SET consecutive_failures = $2,
		    first_failure_at = now() - $3::interval,
		    paused_until = now() + $4::interval
		WHERE id = $1`, endpointID, failures, iv(streakAge), iv(pausedFor))
}

type health struct {
	Failures  int32
	Streak    bool
	Paused    bool
	PausedFor time.Duration // paused_until - now()
	Disabled  bool
}

func getHealth(t *testing.T, endpointID string) health {
	t.Helper()
	var h health
	var pausedForMs *float64
	err := pool.QueryRow(context.Background(), `
		SELECT consecutive_failures, first_failure_at IS NOT NULL,
		       extract(epoch FROM paused_until - now()) * 1000, disabled_at IS NOT NULL
		FROM endpoints WHERE id = $1`, endpointID).Scan(&h.Failures, &h.Streak, &pausedForMs, &h.Disabled)
	if err != nil {
		t.Fatalf("get health %s: %v", endpointID, err)
	}
	if pausedForMs != nil {
		h.Paused = *pausedForMs > 0
		h.PausedFor = time.Duration(*pausedForMs) * time.Millisecond
	}
	return h
}

// waitRequests polls until the endpoint has seen want requests.
func waitRequests(t *testing.T, n *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for n.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("endpoint saw %d requests, want %d", n.Load(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// quiet waits a moment and fails if the endpoint saw more than want requests.
func quiet(t *testing.T, n *atomic.Int32, want int32, why string) {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	if got := n.Load(); got != want {
		t.Errorf("endpoint saw %d requests, want %d (%s)", got, want, why)
	}
}

func endpointOf(t *testing.T, url string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(),
		`SELECT id::text FROM endpoints WHERE url = $1`, url).Scan(&id); err != nil {
		t.Fatalf("find endpoint %s: %v", url, err)
	}
	return id
}

// Invariant 5: a failing endpoint stops taking workers once the breaker opens,
// and deliveries held back by it do not spend attempts.
func TestBreakerOpensAtThreshold(t *testing.T) {
	reset(t)
	srv, requests := countingServer(t, http.StatusServiceUnavailable)
	ep := seedEndpoint(t, srv.URL)
	var held []string
	for i := range 5 {
		id := seedDelivery(t, ep, []byte(`{}`))
		if i >= 3 {
			held = append(held, id)
		}
	}

	startWorker(t, breakerConfig(3))
	waitRequests(t, requests, 3)
	quiet(t, requests, 3, "breaker should be open after 3 failures")

	h := getHealth(t, ep)
	if h.Failures != 3 {
		t.Errorf("consecutive_failures = %d, want 3", h.Failures)
	}
	if !h.Paused || h.PausedFor < 59*time.Minute || h.PausedFor > time.Hour {
		t.Errorf("paused for %v (set=%v), want about BreakerCooldown (1h)", h.PausedFor, h.Paused)
	}
	for _, id := range held {
		if r := get(t, id); r.Status != "pending" || r.AttemptCount != 0 {
			t.Errorf("held delivery = %+v, want pending with 0 attempts", r)
		}
	}
}

// Half-open lets exactly one probe through, even with idle workers and room
// under MaxInFlight.
func TestHalfOpenSendsOneProbe(t *testing.T) {
	reset(t)
	hang := newHangingEndpoint(t, 4)
	setHealth(t, endpointOf(t, hang.srv.URL), 3, 10*time.Minute, -time.Second)

	cfg := hangConfig(4, 4)
	cfg.BreakerThreshold = 3
	cfg.BreakerCooldown = time.Hour
	cfg.BreakerMaxCooldown = 24 * time.Hour
	startWorker(t, cfg)
	t.Cleanup(hang.unblock)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if current, _ := hang.inFlight(); current == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("half-open endpoint never got its probe")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if _, peak := hang.inFlight(); peak != 1 {
		t.Errorf("peak in-flight = %d while half-open, want 1", peak)
	}
}

func TestProbeSuccessClosesBreaker(t *testing.T) {
	reset(t)
	srv, _ := countingServer(t, http.StatusOK)
	ep := seedEndpoint(t, srv.URL)
	setHealth(t, ep, 3, 10*time.Minute, -time.Second)
	ids := []string{seedDelivery(t, ep, []byte(`{}`)), seedDelivery(t, ep, []byte(`{}`))}

	startWorker(t, breakerConfig(3))
	for _, id := range ids {
		waitStatus(t, id, "sent")
	}
	if h := getHealth(t, ep); h.Failures != 0 || h.Streak || h.Paused {
		t.Errorf("health = %+v, want streak and pause cleared", h)
	}
}

// A failed probe reopens the breaker for twice the previous cooldown.
func TestProbeFailureReopensLonger(t *testing.T) {
	reset(t)
	srv, _ := countingServer(t, http.StatusServiceUnavailable)
	ep := seedEndpoint(t, srv.URL)
	setHealth(t, ep, 3, 10*time.Minute, -time.Second)
	id := seedDelivery(t, ep, []byte(`{}`))

	cfg := breakerConfig(3)
	cfg.BreakerCooldown = time.Minute
	startWorker(t, cfg)
	waitAttempts(t, id, 1)

	h := getHealth(t, ep)
	if h.Failures != 4 {
		t.Errorf("consecutive_failures = %d, want 4", h.Failures)
	}
	if h.PausedFor < 119*time.Second || h.PausedFor > 2*time.Minute {
		t.Errorf("paused for %v, want about 2m (cooldown doubled)", h.PausedFor)
	}
}

func TestSuccessResetsStreak(t *testing.T) {
	reset(t)
	srv, _ := countingServer(t, http.StatusOK)
	ep := seedEndpoint(t, srv.URL)
	setHealth(t, ep, 2, 10*time.Minute, -time.Second)
	id := seedDelivery(t, ep, []byte(`{}`))

	startWorker(t, breakerConfig(3))
	waitStatus(t, id, "sent")
	if h := getHealth(t, ep); h.Failures != 0 || h.Streak {
		t.Errorf("health = %+v, want streak cleared", h)
	}
}

// Invariant 1: a disabled endpoint's backlog is held, not dropped, and no
// worker spends time on it.
func TestDisabledEndpointIsNeverClaimed(t *testing.T) {
	reset(t)
	srv, requests := countingServer(t, http.StatusOK)
	ep := seedEndpoint(t, srv.URL)
	exec(t, `UPDATE endpoints SET disabled_at = now() WHERE id = $1`, ep)
	id := seedDelivery(t, ep, []byte(`{}`))

	startWorker(t, breakerConfig(3))
	quiet(t, requests, 0, "endpoint is disabled")
	if r := get(t, id); r.Status != "pending" || r.AttemptCount != 0 {
		t.Errorf("delivery = %+v, want pending with 0 attempts", r)
	}
}

func TestLongStreakDisablesEndpoint(t *testing.T) {
	reset(t)
	srv, requests := countingServer(t, http.StatusServiceUnavailable)
	ep := seedEndpoint(t, srv.URL)
	setHealth(t, ep, 1, 2*time.Hour, -time.Second)
	first := seedDelivery(t, ep, []byte(`{"n":1}`))
	second := seedDelivery(t, ep, []byte(`{"n":2}`))

	logs := &logBuffer{}
	cfg := breakerConfig(100) // keep the breaker out of it
	cfg.DisableAfter = time.Hour
	cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
	startWorker(t, cfg)

	waitAttempts(t, first, 1)
	quiet(t, requests, 1, "endpoint disabled after the first failure")

	if h := getHealth(t, ep); !h.Disabled {
		t.Errorf("health = %+v, want disabled", h)
	}
	// The failed delivery keeps its retry schedule; both wait for re-enable.
	if r := get(t, first); r.Status != "pending" || r.AttemptCount != 1 {
		t.Errorf("failed delivery = %+v, want pending with 1 attempt", r)
	}
	if r := get(t, second); r.Status != "pending" || r.AttemptCount != 0 {
		t.Errorf("held delivery = %+v, want pending with 0 attempts", r)
	}
	if !logs.contains("endpoint disabled") || !logs.contains(ep) {
		t.Error(`no "endpoint disabled" log with the endpoint_id`)
	}
}

// Fencing: an outcome dropped for a lost lease must not count against the
// endpoint either.
func TestLostLeaseLeavesHealthAlone(t *testing.T) {
	reset(t)
	stolen := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = pool.Exec(r.Context(), `UPDATE deliveries SET lease_id = uuidv7() WHERE status = 'processing'`)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(stolen.Close)
	ok, _ := countingServer(t, http.StatusOK)

	ep := seedEndpoint(t, stolen.URL)
	seedDelivery(t, ep, []byte(`{}`))
	startWorker(t, breakerConfig(3))

	// One loop: once a later delivery is sent, the stale outcome was handled.
	time.Sleep(200 * time.Millisecond)
	waitStatus(t, seed(t, ok.URL, []byte(`{}`)), "sent")

	if h := getHealth(t, ep); h.Failures != 0 || h.Streak {
		t.Errorf("health = %+v, want untouched (lease was lost)", h)
	}
}

// A discarded delivery never reached the endpoint, so it says nothing about
// the endpoint's health.
func TestDiscardedLeavesHealthAlone(t *testing.T) {
	reset(t)
	ep := seedEndpoint(t, "http://bad host/")
	id := seedDelivery(t, ep, []byte(`{}`))

	startWorker(t, breakerConfig(1))
	waitStatus(t, id, "discarded")
	if h := getHealth(t, ep); h.Failures != 0 || h.Streak {
		t.Errorf("health = %+v, want untouched (discarded)", h)
	}
}
