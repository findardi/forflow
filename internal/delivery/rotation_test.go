//go:build integration

package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"forflow/internal/signing"
	"forflow/internal/store/sqlc"
)

// verifyWith checks a received request against one secret, as a receiver
// holding only that secret would.
func verifyWith(secret []byte, r received) error {
	return signing.Verify(secret,
		r.header.Get("webhook-id"), r.header.Get("webhook-timestamp"), r.header.Get("webhook-signature"),
		r.body, time.Now(), 5*time.Minute)
}

// During a rotation a receiver may hold either secret, so every attempt must
// verify with both; once the grace period ends only the new one signs.
func TestRotationSignsWithBothSecretsUntilExpiry(t *testing.T) {
	reset(t)
	srv, reqs := recordingServer(t)
	endpointID := seedEndpoint(t, srv.URL)

	var id pgtype.UUID
	if err := id.Scan(endpointID); err != nil {
		t.Fatalf("parse endpoint id: %v", err)
	}
	newSecret := bytes.Repeat([]byte{'n'}, 32)
	n, err := sqlc.New(pool).RotateEndpointSecret(context.Background(),
		sqlc.RotateEndpointSecretParams{ID: id, Secret: newSecret})
	if err != nil || n != 1 {
		t.Fatalf("rotate: rows %d, err %v", n, err)
	}
	var graceLeftMs float64
	if err := pool.QueryRow(context.Background(),
		`SELECT extract(epoch FROM previous_secret_expires_at - now()) * 1000 FROM endpoints WHERE id = $1`,
		endpointID).Scan(&graceLeftMs); err != nil {
		t.Fatalf("read grace period: %v", err)
	}
	if graceLeft := time.Duration(graceLeftMs) * time.Millisecond; graceLeft < 23*time.Hour || graceLeft > 24*time.Hour {
		t.Errorf("previous secret expires in %v, want 24h", graceLeft)
	}

	startWorker(t, testConfig)
	waitStatus(t, seedDelivery(t, endpointID, []byte(`{"n":1}`)), "sent")
	during := <-reqs
	if err := verifyWith(newSecret, during); err != nil {
		t.Errorf("during grace: new secret: %v", err)
	}
	if err := verifyWith(testSecret, during); err != nil {
		t.Errorf("during grace: previous secret: %v", err)
	}

	exec(t, `UPDATE endpoints SET previous_secret_expires_at = now() - interval '1 second' WHERE id = $1`, endpointID)
	waitStatus(t, seedDelivery(t, endpointID, []byte(`{"n":2}`)), "sent")
	after := <-reqs
	if got := len(strings.Fields(after.header.Get("webhook-signature"))); got != 1 {
		t.Errorf("after grace: %d signatures, want 1", got)
	}
	if err := verifyWith(newSecret, after); err != nil {
		t.Errorf("after grace: new secret: %v", err)
	}
	if err := verifyWith(testSecret, after); !errors.Is(err, signing.ErrNotMatchSignature) {
		t.Errorf("after grace: previous secret err = %v, want ErrNotMatchSignature", err)
	}
}

// webhook-delivery: abort-message is the receiver saying "never send this
// again". The delivery fails at once, keeps its attempt row, and the deliberate
// reply does not count against the endpoint's health.
func TestAbortMessageStopsRetries(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("webhook-delivery", "abort-message")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)
	r := waitStatus(t, id, "failed")

	if r.AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", r.AttemptCount)
	}
	got := attemptsOf(t, id)
	if len(got) != 1 {
		t.Fatalf("got %d attempts %v, want 1", len(got), got)
	}
	if want := "#1 succeeded=false response_status=400 error_kind=NULL"; got[0].String() != want {
		t.Errorf("attempt = %s, want %s", got[0], want)
	}
	if h := getHealth(t, endpointOf(t, srv.URL)); h.Failures != 0 {
		t.Errorf("consecutive_failures = %d, want 0", h.Failures)
	}
}

// The header only means something on a failure; a 2xx is delivered as usual.
func TestAbortHeaderOnSuccessIsDelivered(t *testing.T) {
	reset(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("webhook-delivery", "abort-message")
	}))
	defer srv.Close()

	id := seed(t, srv.URL, []byte(`{}`))
	startWorker(t, testConfig)
	waitStatus(t, id, "sent")
}
