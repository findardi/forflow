//go:build integration

package delivery_test

// These tests are the spec for the v0 worker. The implementation is
// owner-written (red zone); the tests only fix the public surface:
//
//	delivery.New(pool, delivery.Config{...}) *Worker
//	(*Worker).Run(ctx) error  // returns nil once ctx is cancelled and in-flight work is done

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"forflow/internal/delivery"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

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

	if err := migrateUp(ctx); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}
	return m.Run()
}

// migrateUp runs the Up section of every migration in order. It avoids a
// goose dependency in tests; the files are tiny and sorted by name.
func migrateUp(ctx context.Context) error {
	for _, name := range []string{"00001_init.sql", "00002_delivery_lease.sql"} {
		b, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			return err
		}
		up, _, _ := strings.Cut(string(b), "-- +goose Down")
		if _, err := pool.Exec(ctx, up); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

var testConfig = delivery.Config{
	HTTPTimeout:  500 * time.Millisecond,
	Lease:        5 * time.Second,
	PollInterval: 50 * time.Millisecond,
}

// startWorker runs w until the test ends and returns a stop func that
// cancels it and reports how long Run took to return.
func startWorker(t *testing.T, cfg delivery.Config) (stop func() (time.Duration, error)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- delivery.New(pool, cfg).Run(ctx) }()

	var once sync.Once
	var elapsed time.Duration
	var runErr error
	stop = func() (time.Duration, error) {
		once.Do(func() {
			start := time.Now()
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(10 * time.Second):
				runErr = fmt.Errorf("Run did not return within 10s of cancel")
			}
			elapsed = time.Since(start)
		})
		return elapsed, runErr
	}
	t.Cleanup(func() { _, _ = stop() })
	return stop
}

func reset(t *testing.T) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "TRUNCATE deliveries, messages, endpoints"); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// seed inserts one endpoint, one message and their pending delivery.
func seed(t *testing.T, url string, payload []byte) (deliveryID string) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		WITH e AS (INSERT INTO endpoints (url) VALUES ($1) RETURNING id),
		     m AS (INSERT INTO messages (payload) VALUES ($2) RETURNING id)
		INSERT INTO deliveries (message_id, endpoint_id)
		SELECT m.id, e.id FROM m, e
		RETURNING id::text`, url, payload).Scan(&deliveryID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return deliveryID
}

type row struct {
	Status         string
	ResponseStatus *int32
	Attempted      bool
}

func get(t *testing.T, id string) row {
	t.Helper()
	var r row
	err := pool.QueryRow(context.Background(),
		`SELECT status, response_status, attempted_at IS NOT NULL FROM deliveries WHERE id = $1`, id,
	).Scan(&r.Status, &r.ResponseStatus, &r.Attempted)
	if err != nil {
		t.Fatalf("get delivery %s: %v", id, err)
	}
	return r
}

// waitStatus polls until the delivery reaches want or the deadline passes.
func waitStatus(t *testing.T, id, want string) row {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := get(t, id)
		if r.Status == want {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery %s: status %q, want %q", id, r.Status, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDeliveryOutcome(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus string
		wantCode   *int32
	}{
		{
			name:       "2xx marks sent",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
			wantStatus: "sent",
			wantCode:   ptr(200),
		},
		{
			name:       "204 also counts as sent",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) },
			wantStatus: "sent",
			wantCode:   ptr(204),
		},
		{
			name:       "5xx marks failed",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			wantStatus: "failed",
			wantCode:   ptr(500),
		},
		{
			name:       "4xx marks failed",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusGone) },
			wantStatus: "failed",
			wantCode:   ptr(410),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reset(t)
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			id := seed(t, srv.URL, []byte(`{}`))

			startWorker(t, testConfig)
			r := waitStatus(t, id, tt.wantStatus)

			if r.ResponseStatus == nil || *r.ResponseStatus != *tt.wantCode {
				t.Errorf("response_status = %v, want %d", deref(r.ResponseStatus), *tt.wantCode)
			}
			if !r.Attempted {
				t.Error("attempted_at is NULL")
			}
		})
	}
}

func TestConnectionRefusedMarksFailed(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens on this port any more

	id := seed(t, url, []byte(`{}`))
	startWorker(t, testConfig)
	r := waitStatus(t, id, "failed")

	if r.ResponseStatus != nil {
		t.Errorf("response_status = %d, want NULL (no response was received)", *r.ResponseStatus)
	}
}

func TestHangingEndpointTimesOut(t *testing.T) {
	reset(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)

	// HTTPTimeout is 500ms; failing well before the 10s wait proves the
	// client has a timeout instead of waiting forever.
	r := waitStatus(t, id, "failed")
	if r.ResponseStatus != nil {
		t.Errorf("response_status = %d, want NULL on timeout", *r.ResponseStatus)
	}
}

// Invariant 4: the endpoint receives exactly the stored bytes.
func TestSendsExactPayloadBytes(t *testing.T) {
	reset(t)
	payload := []byte(`{"b":1,  "a":[2, 3]}`)

	got := make(chan *http.Request, 1)
	body := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- r
		body <- b
	}))
	defer srv.Close()

	id := seed(t, srv.URL, payload)
	startWorker(t, testConfig)
	waitStatus(t, id, "sent")

	r := <-got
	if r.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", r.Method)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if b := <-body; string(b) != string(payload) {
		t.Errorf("body = %q, want %q", b, payload)
	}
}

// Invariant 2: two workers never deliver the same row twice.
func TestTwoWorkersDeliverEachRowOnce(t *testing.T) {
	reset(t)
	const n = 100

	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		time.Sleep(5 * time.Millisecond) // widen the race window
		mu.Lock()
		hits[string(b)]++
		mu.Unlock()
	}))
	defer srv.Close()

	ids := make([]string, n)
	for i := range n {
		ids[i] = seed(t, srv.URL, fmt.Appendf(nil, `{"n":%d}`, i))
	}

	startWorker(t, testConfig)
	startWorker(t, testConfig)
	for _, id := range ids {
		waitStatus(t, id, "sent")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(hits) != n {
		t.Errorf("endpoint saw %d distinct payloads, want %d", len(hits), n)
	}
	for body, c := range hits {
		if c != 1 {
			t.Errorf("payload %s delivered %d times, want 1", body, c)
		}
	}
}

// Option B crash recovery: a lease left behind by a dead worker is picked up.
func TestReclaimsExpiredLease(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	setLease(t, id, "now() - interval '1 minute'")

	startWorker(t, testConfig)
	waitStatus(t, id, "sent")
}

// A live lease belongs to another worker and must be left alone.
func TestSkipsActiveLease(t *testing.T) {
	reset(t)
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	setLease(t, id, "now() + interval '1 hour'")

	startWorker(t, testConfig)
	time.Sleep(20 * testConfig.PollInterval)

	if r := get(t, id); r.Status != "processing" {
		t.Errorf("status = %q, want processing (lease still active)", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Errorf("endpoint called %d times, want 0", calls)
	}
}

// Invariant 6: on cancel, Run finishes the in-flight attempt, then returns.
func TestShutdownFinishesInFlight(t *testing.T) {
	reset(t)
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	stop := startWorker(t, testConfig)

	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never called the endpoint")
	}
	elapsed, err := stop()
	if err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	// Bounded: the in-flight request may take at most HTTPTimeout.
	if limit := testConfig.HTTPTimeout + time.Second; elapsed > limit {
		t.Errorf("Run took %v to return after cancel, want <= %v", elapsed, limit)
	}
	if r := get(t, id); r.Status != "sent" {
		t.Errorf("status = %q, want sent (in-flight attempt must complete)", r.Status)
	}
}

func setLease(t *testing.T, id, until string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE deliveries SET status = 'processing', locked_until = `+until+` WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("set lease: %v", err)
	}
}

func ptr(v int32) *int32 { return &v }

func deref(p *int32) any {
	if p == nil {
		return "NULL"
	}
	return *p
}
