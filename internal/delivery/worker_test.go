//go:build integration

package delivery_test

// These tests are the spec for the v0 worker. The implementation is
// owner-written (red zone); the tests only fix the public surface:
//
//	delivery.New(pool, delivery.Config{...}) *Worker
//	(*Worker).Run(ctx) error  // returns nil once ctx is cancelled and in-flight work is done

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"forflow/internal/delivery"
	"forflow/internal/signing"
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
// goose dependency in tests; the files are tiny and Glob returns them sorted.
func migrateUp(ctx context.Context) error {
	names, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		return err
	}
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		up, _, _ := strings.Cut(string(b), "-- +goose Down")
		// goose statement markers are SQL comments, so the section runs as is.
		if _, err := pool.Exec(ctx, up); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// The breaker is set far out of reach so tests that predate it keep testing
// retries alone; breaker_test.go narrows it. Transport skips netguard, which
// would refuse every httptest server on loopback.
var testConfig = delivery.Config{
	Transport:          http.DefaultTransport,
	HTTPTimeout:        500 * time.Millisecond,
	Lease:              5 * time.Second,
	PollInterval:       50 * time.Millisecond,
	Workers:            1,
	MaxInFlight:        1,
	BreakerThreshold:   1000,
	BreakerCooldown:    time.Second,
	BreakerMaxCooldown: time.Second,
	DisableAfter:       1000 * time.Hour,
}

// startWorker runs w until the test ends and returns a stop func that
// cancels it and reports how long Run took to return.
func startWorker(t *testing.T, cfg delivery.Config) (stop func() (time.Duration, error)) {
	t.Helper()
	return runWorker(t, delivery.New(pool, cfg))
}

// runWorker is startWorker for a test that needs the Worker itself.
func runWorker(t *testing.T, w *delivery.Worker) (stop func() (time.Duration, error)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

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
	if _, err := pool.Exec(context.Background(), "TRUNCATE attempts, deliveries, messages, endpoints"); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// testSecret is the signing key of every seeded endpoint.
var testSecret = []byte("forflow-test-signing-secret-32b!")

// seed inserts one endpoint, one message and their pending delivery.
func seed(t *testing.T, url string, payload []byte) (deliveryID string) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		WITH e AS (INSERT INTO endpoints (url, secret) VALUES ($1, $3) RETURNING id),
		     m AS (INSERT INTO messages (payload) VALUES ($2) RETURNING id)
		INSERT INTO deliveries (message_id, endpoint_id)
		SELECT m.id, e.id FROM m, e
		RETURNING id::text`, url, payload, testSecret).Scan(&deliveryID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return deliveryID
}

type row struct {
	Status         string
	ResponseStatus *int32
	Attempted      bool
	AttemptCount   int32
	DueIn          time.Duration // next_attempt_at - now()
}

func get(t *testing.T, id string) row {
	t.Helper()
	var r row
	var dueInMs float64
	err := pool.QueryRow(context.Background(), `
		SELECT status, response_status, attempted_at IS NOT NULL, attempt_count,
		       extract(epoch FROM next_attempt_at - now()) * 1000
		FROM deliveries WHERE id = $1`, id,
	).Scan(&r.Status, &r.ResponseStatus, &r.Attempted, &r.AttemptCount, &dueInMs)
	if err != nil {
		t.Fatalf("get delivery %s: %v", id, err)
	}
	r.DueIn = time.Duration(dueInMs) * time.Millisecond
	return r
}

// exec runs a fixture statement against the test database.
func exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// waitAttempts polls until the delivery has recorded want attempts.
func waitAttempts(t *testing.T, id string, want int32) row {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := get(t, id)
		if r.AttemptCount >= want && r.Status != "processing" {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery %s: %d attempts (status %q), want %d", id, r.AttemptCount, r.Status, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
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

// Invariant 1: a failed attempt is rescheduled, never dropped. Every
// non-2xx answer and every transport failure counts as a failed attempt.
func TestEndpointFailureSchedulesRetry(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc // nil: connection refused
		wantCode *int32
	}{
		{
			name:     "5xx",
			handler:  func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			wantCode: ptr(500),
		},
		{
			name:     "4xx",
			handler:  func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusGone) },
			wantCode: ptr(410),
		},
		{
			name: "3xx is not followed",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://example.test/elsewhere", http.StatusFound)
			},
			wantCode: ptr(302),
		},
		{
			// HTTPTimeout is 500ms; a retry well before the 10s wait proves
			// the client has a timeout instead of waiting forever.
			name: "hanging endpoint times out",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Bounded so srv.Close does not wait on this handler forever:
				// the server may not notice the client giving up.
				select {
				case <-r.Context().Done():
				case <-time.After(2 * testConfig.HTTPTimeout):
				}
			},
		},
		{name: "connection refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reset(t)
			handler := tt.handler
			if handler == nil {
				handler = http.NotFoundHandler().ServeHTTP
			}
			srv := httptest.NewServer(handler)
			defer srv.Close()
			if tt.handler == nil {
				srv.Close() // nothing listens on this port any more
			}
			id := seed(t, srv.URL, []byte(`{}`))

			startWorker(t, testConfig)
			r := waitAttempts(t, id, 1)

			if r.Status != "pending" {
				t.Errorf("status = %q, want pending (retry scheduled)", r.Status)
			}
			if r.AttemptCount != 1 {
				t.Errorf("attempt_count = %d, want 1", r.AttemptCount)
			}
			// First retry waits 5s plus up to 10% jitter.
			if r.DueIn < 3*time.Second || r.DueIn > 6*time.Second {
				t.Errorf("next attempt due in %v, want about 5s", r.DueIn)
			}
			if deref(r.ResponseStatus) != deref(tt.wantCode) {
				t.Errorf("response_status = %v, want %v", deref(r.ResponseStatus), deref(tt.wantCode))
			}
		})
	}
}

// A due retry is claimed again and its success is recorded.
func TestDueRetryIsDelivered(t *testing.T) {
	reset(t)
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)
	waitAttempts(t, id, 1)

	// Fast-forward the schedule instead of waiting 5s.
	exec(t, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, id)
	r := waitStatus(t, id, "sent")

	if r.AttemptCount != 2 {
		t.Errorf("attempt_count = %d, want 2", r.AttemptCount)
	}
}

func TestExhaustedRetriesMarkFailed(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	// Seven attempts already failed; the next one is the last.
	exec(t, `UPDATE deliveries SET attempt_count = 7 WHERE id = $1`, id)

	startWorker(t, testConfig)
	r := waitStatus(t, id, "failed")

	if r.AttemptCount != 8 {
		t.Errorf("attempt_count = %d, want 8", r.AttemptCount)
	}
}

// An unparseable endpoint URL is our failure, not the endpoint's: the row is
// discarded without spending an attempt.
func TestInvalidURLIsDiscarded(t *testing.T) {
	reset(t)
	id := seed(t, "http://bad host/", []byte(`{}`))

	startWorker(t, testConfig)
	r := waitStatus(t, id, "discarded")

	if r.AttemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", r.AttemptCount)
	}
}

// Fencing: when the lease changes hands mid-attempt, the stale worker's
// result is dropped and the worker keeps running.
func TestLostLeaseDropsResult(t *testing.T) {
	reset(t)
	const stolen = `{"n":1}`
	stealErr := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != stolen {
			return
		}
		// Simulate another worker reclaiming the row while this one sends.
		_, err := pool.Exec(r.Context(), `
			UPDATE deliveries SET lease_id = uuidv7()
			WHERE message_id IN (SELECT id FROM messages WHERE payload = $1)`, b)
		stealErr <- err
	}))
	defer srv.Close()

	stolenID := seed(t, srv.URL, []byte(stolen))
	startWorker(t, testConfig)

	select {
	case err := <-stealErr:
		if err != nil {
			t.Fatalf("steal lease: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker never called the endpoint")
	}

	// The worker handles one row at a time, so once the next row is sent the
	// stale outcome has been recorded (or dropped). It also proves the worker
	// survived the lost lease.
	next := seed(t, srv.URL, []byte(`{"n":2}`))
	waitStatus(t, next, "sent")

	r := get(t, stolenID)
	if r.Status != "processing" || r.Attempted || r.AttemptCount != 0 {
		t.Errorf("stolen row = %+v, want untouched processing row", r)
	}
	// The new lease holder logs its own attempt; the stale one must not.
	if n := len(attemptsOf(t, stolenID)); n != 0 {
		t.Errorf("stolen row has %d attempts, want 0", n)
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

// received is what one webhook request looked like on the wire.
type received struct {
	header http.Header
	body   []byte
}

// recordingServer answers each request with the next status in statuses
// (200 once they run out) and records every request it gets.
func recordingServer(t *testing.T, statuses ...int) (*httptest.Server, <-chan received) {
	t.Helper()
	reqs := make(chan received, 10)
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		reqs <- received{header: r.Header.Clone(), body: b}
		mu.Lock()
		defer mu.Unlock()
		if len(statuses) > 0 {
			w.WriteHeader(statuses[0])
			statuses = statuses[1:]
		}
	}))
	t.Cleanup(srv.Close)
	return srv, reqs
}

func messageID(t *testing.T, deliveryID string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(),
		`SELECT message_id::text FROM deliveries WHERE id = $1`, deliveryID).Scan(&id); err != nil {
		t.Fatalf("message id: %v", err)
	}
	return id
}

// verify checks a received request the way a consumer would.
func verify(t *testing.T, r received) {
	t.Helper()
	err := signing.Verify(testSecret,
		r.header.Get("webhook-id"), r.header.Get("webhook-timestamp"), r.header.Get("webhook-signature"),
		r.body, time.Now(), 5*time.Minute)
	if err != nil {
		t.Errorf("consumer verification failed: %v", err)
	}
}

// Invariant 4: the signature covers the exact raw body bytes that are sent.
func TestSignatureCoversSentBody(t *testing.T) {
	reset(t)
	// Odd spacing and key order: any re-encoding would break the signature.
	payload := []byte(`{"b":1,  "a":[2, 3]}`)
	srv, reqs := recordingServer(t)

	id := seed(t, srv.URL, payload)
	startWorker(t, testConfig)
	waitStatus(t, id, "sent")

	r := <-reqs
	if got, want := r.header.Get("webhook-id"), messageID(t, id); got != want {
		t.Errorf("webhook-id = %q, want message id %q", got, want)
	}
	if string(r.body) != string(payload) {
		t.Errorf("body = %q, want %q", r.body, payload)
	}
	verify(t, r)
}

// Invariant 3: webhook-id is stable across retries; webhook-timestamp (and so
// the signature) is fresh on every attempt.
func TestRetryKeepsIDRefreshesTimestamp(t *testing.T) {
	reset(t)
	srv, reqs := recordingServer(t, http.StatusServiceUnavailable)

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)
	waitAttempts(t, id, 1)

	// The header has second resolution; cross a second boundary before the
	// retry so a fresh timestamp is observable.
	time.Sleep(1100 * time.Millisecond)
	exec(t, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, id)
	waitStatus(t, id, "sent")

	first, retry := <-reqs, <-reqs
	want := messageID(t, id)
	for i, r := range []received{first, retry} {
		if got := r.header.Get("webhook-id"); got != want {
			t.Errorf("attempt %d: webhook-id = %q, want %q", i+1, got, want)
		}
		verify(t, r)
	}

	t1, err1 := strconv.ParseInt(first.header.Get("webhook-timestamp"), 10, 64)
	t2, err2 := strconv.ParseInt(retry.header.Get("webhook-timestamp"), 10, 64)
	if err1 != nil || err2 != nil {
		t.Fatalf("timestamps not integers: %v, %v", err1, err2)
	}
	if t2 <= t1 {
		t.Errorf("retry webhook-timestamp = %d, want later than first attempt %d", t2, t1)
	}
	if first.header.Get("webhook-signature") == retry.header.Get("webhook-signature") {
		t.Error("retry reused the first attempt's signature")
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

// setLease makes the row look claimed by another worker whose lease ends at until.
func setLease(t *testing.T, id, until string) {
	t.Helper()
	exec(t, `UPDATE deliveries
		SET status = 'processing', lease_id = uuidv7(),
		    next_attempt_at = `+until+`
		WHERE id = $1`, id)
}

func ptr(v int32) *int32 { return &v }

func deref(p *int32) any {
	if p == nil {
		return "NULL"
	}
	return *p
}

// seedEndpoint inserts one endpoint, for tests that send several deliveries to it.
func seedEndpoint(t *testing.T, url string) (endpointID string) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`INSERT INTO endpoints (url, secret) VALUES ($1, $2) RETURNING id::text`,
		url, testSecret).Scan(&endpointID)
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	return endpointID
}

// seedDelivery inserts one message and its pending delivery to endpointID.
func seedDelivery(t *testing.T, endpointID string, payload []byte) (deliveryID string) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		WITH m AS (INSERT INTO messages (payload) VALUES ($2) RETURNING id)
		INSERT INTO deliveries (message_id, endpoint_id)
		SELECT m.id, $1 FROM m
		RETURNING id::text`, endpointID, payload).Scan(&deliveryID)
	if err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	return deliveryID
}

// hangingEndpoint is a customer endpoint that accepts connections but never
// replies until released, and records how many requests it held at once.
type hangingEndpoint struct {
	srv     *httptest.Server
	release chan struct{}

	mu      sync.Mutex
	current int
	peak    int
}

// newHangingEndpoint starts the endpoint with n due deliveries that are older
// than anything else, so a first-come claim order would pick all of them first.
func newHangingEndpoint(t *testing.T, n int) *hangingEndpoint {
	t.Helper()
	h := &hangingEndpoint{release: make(chan struct{})}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.current++
		h.peak = max(h.peak, h.current)
		h.mu.Unlock()
		defer func() {
			h.mu.Lock()
			h.current--
			h.mu.Unlock()
		}()
		select {
		case <-h.release:
		case <-r.Context().Done():
		}
	}))
	// Registered first, so it runs after the worker has stopped.
	t.Cleanup(h.srv.Close)

	endpointID := seedEndpoint(t, h.srv.URL)
	for i := range n {
		seedDelivery(t, endpointID, fmt.Appendf(nil, `{"hang":%d}`, i))
	}
	exec(t, `UPDATE deliveries SET next_attempt_at = now() - interval '1 minute' WHERE endpoint_id = $1`, endpointID)
	return h
}

// unblock lets held requests finish. Register it after startWorker, so it runs
// before the worker's stop and shutdown does not wait out HTTPTimeout.
func (h *hangingEndpoint) unblock() { close(h.release) }

func (h *hangingEndpoint) inFlight() (current, peak int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current, h.peak
}

// hangConfig gives a hanging endpoint a timeout far longer than a healthy
// delivery takes, so being stuck behind it is unmistakable.
func hangConfig(workers int, maxInFlight int32) delivery.Config {
	cfg := testConfig
	cfg.HTTPTimeout = 5 * time.Second
	cfg.Lease = 10 * time.Second
	cfg.Workers = workers
	cfg.MaxInFlight = maxInFlight
	return cfg
}

// Invariant 5: a hanging endpoint with a backlog must not delay deliveries to
// other endpoints. Without the per-endpoint cap, the hanging endpoint's older
// rows would take every worker for a full HTTPTimeout.
func TestHangingEndpointDoesNotDelayOthers(t *testing.T) {
	reset(t)
	hang := newHangingEndpoint(t, 10)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(healthy.Close)
	id := seed(t, healthy.URL, []byte(`{}`))

	cfg := hangConfig(3, 1)
	start := time.Now()
	startWorker(t, cfg)
	t.Cleanup(hang.unblock)

	waitStatus(t, id, "sent")
	if elapsed, limit := time.Since(start), cfg.HTTPTimeout/2; elapsed > limit {
		t.Errorf("healthy delivery sent after %v, want < %v (stuck behind the hanging endpoint)", elapsed, limit)
	}
}

// Invariant 5: an endpoint never holds more than MaxInFlight workers, even
// when the rest of the pool is idle.
func TestCapLimitsInFlightPerEndpoint(t *testing.T) {
	reset(t)
	hang := newHangingEndpoint(t, 10)

	cfg := hangConfig(4, 2)
	startWorker(t, cfg)
	t.Cleanup(hang.unblock)

	deadline := time.Now().Add(cfg.HTTPTimeout / 2)
	for {
		if current, _ := hang.inFlight(); current >= int(cfg.MaxInFlight) {
			break
		}
		if time.Now().After(deadline) {
			current, _ := hang.inFlight()
			t.Fatalf("endpoint holds %d requests, want %d (pool not running in parallel)", current, cfg.MaxInFlight)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give the idle workers several poll cycles to (wrongly) claim more.
	time.Sleep(20 * cfg.PollInterval)

	if _, peak := hang.inFlight(); peak != int(cfg.MaxInFlight) {
		t.Errorf("endpoint held up to %d requests at once, want at most %d", peak, cfg.MaxInFlight)
	}
}

type attemptRow struct {
	Number         int32
	Succeeded      bool
	ResponseStatus *int32
	ErrorKind      *string
	CreatedAt      time.Time
}

func (a attemptRow) String() string {
	kind := "NULL"
	if a.ErrorKind != nil {
		kind = *a.ErrorKind
	}
	return fmt.Sprintf("#%d succeeded=%v response_status=%v error_kind=%s",
		a.Number, a.Succeeded, deref(a.ResponseStatus), kind)
}

// attemptsOf returns the delivery's attempt log, oldest first.
func attemptsOf(t *testing.T, deliveryID string) []attemptRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT attempt_number, succeeded, response_status, error_kind, created_at
		FROM attempts WHERE delivery_id = $1
		ORDER BY created_at, id`, deliveryID)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[attemptRow])
	if err != nil {
		t.Fatalf("scan attempts: %v", err)
	}
	return got
}

// Every contact with the endpoint leaves its own attempts row, so a retried
// delivery keeps its whole history instead of only the last result.
func TestEachAttemptIsLogged(t *testing.T) {
	reset(t)
	srv, _ := recordingServer(t, http.StatusServiceUnavailable)

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)
	waitAttempts(t, id, 1)
	exec(t, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, id)
	waitStatus(t, id, "sent")

	got := attemptsOf(t, id)
	want := []string{
		"#1 succeeded=false response_status=503 error_kind=NULL",
		"#2 succeeded=true response_status=200 error_kind=NULL",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d attempts %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("attempt %d = %s, want %s", i+1, got[i], want[i])
		}
	}
	if !got[0].CreatedAt.Before(got[1].CreatedAt) {
		t.Errorf("created_at %v then %v, want increasing", got[0].CreatedAt, got[1].CreatedAt)
	}
}

// Without a Transport the worker uses netguard: a loopback endpoint is never
// contacted, and the refusal is an endpoint failure, so retries and the
// breaker apply as for any unreachable endpoint.
func TestBlockedEndpointIsNeverContacted(t *testing.T) {
	reset(t)
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	cfg := testConfig
	cfg.Transport = nil
	startWorker(t, cfg)
	r := waitAttempts(t, id, 1)

	if r.Status != "pending" {
		t.Errorf("status = %q, want pending (retry scheduled)", r.Status)
	}
	got := attemptsOf(t, id)
	if len(got) != 1 {
		t.Fatalf("got %d attempts %v, want 1", len(got), got)
	}
	if want := "#1 succeeded=false response_status=NULL error_kind=blocked"; got[0].String() != want {
		t.Errorf("attempt = %s, want %s", got[0], want)
	}
	var failures int32
	if err := pool.QueryRow(context.Background(), `
		SELECT e.consecutive_failures FROM endpoints e
		JOIN deliveries d ON d.endpoint_id = e.id WHERE d.id = $1`, id).Scan(&failures); err != nil {
		t.Fatalf("query endpoint health: %v", err)
	}
	if failures != 1 {
		t.Errorf("consecutive_failures = %d, want 1", failures)
	}
	if hit.Load() {
		t.Error("loopback endpoint received the request")
	}
}

// A transport failure has no response, so the attempt carries its error label
// and the time the attempt started.
func TestTransportFailureIsLogged(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close() // nothing listens on this port any more

	id := seed(t, srv.URL, []byte(`{}`))
	start := time.Now()
	startWorker(t, testConfig)
	waitAttempts(t, id, 1)

	got := attemptsOf(t, id)
	if len(got) != 1 {
		t.Fatalf("got %d attempts %v, want 1", len(got), got)
	}
	if want := "#1 succeeded=false response_status=NULL error_kind=conn_refused"; got[0].String() != want {
		t.Errorf("attempt = %s, want %s", got[0], want)
	}
	if at := got[0].CreatedAt; at.Before(start.Add(-time.Second)) || at.After(time.Now()) {
		t.Errorf("created_at = %v, want the attempt start (after %v)", at, start)
	}
}

// logBuffer collects worker logs so a test can wait for a specific event.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Contains(l.b.Bytes(), []byte(s))
}

// Invariant 1: when the attempt cannot be logged (here: no partition), the
// outcome rolls back with it. The lease expires and the delivery is sent again
// without spending an attempt, instead of ending up sent with no history.
func TestUnloggableAttemptIsRetried(t *testing.T) {
	reset(t)
	exec(t, dropAllAttemptPartitions)
	t.Cleanup(func() { exec(t, `SELECT ensure_attempt_partitions(3)`) })

	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	logs := &logBuffer{}
	cfg := testConfig
	cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
	startWorker(t, cfg)

	// Restoring partitions before record has failed would let it succeed.
	deadline := time.Now().Add(10 * time.Second)
	for !logs.contains("insert attempt") {
		if time.Now().After(deadline) {
			t.Fatal("worker never failed to insert the attempt")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r := get(t, id); r.Status != "processing" || r.AttemptCount != 0 {
		t.Fatalf("after failed insert: %+v, want processing with 0 attempts (outcome rolled back)", r)
	}

	exec(t, `SELECT ensure_attempt_partitions(3)`)
	// Expire the lease instead of waiting it out.
	exec(t, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, id)
	r := waitStatus(t, id, "sent")

	if r.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1 (the failed record must not spend an attempt)", r.AttemptCount)
	}
	if n := len(attemptsOf(t, id)); n != 1 {
		t.Errorf("got %d attempts, want 1", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 2 {
		t.Errorf("endpoint called %d times, want 2 (sent again after the rollback)", hits)
	}
}
