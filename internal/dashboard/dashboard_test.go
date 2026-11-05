//go:build integration

package dashboard_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/dashboard"
	"forflow/internal/signing"
	"forflow/internal/store/pgtest"
	"forflow/internal/store/sqlc"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) { pgtest.Main(m, &pool) }

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "TRUNCATE attempts, deliveries, messages, endpoints"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	srv := httptest.NewServer(dashboard.New(sqlc.New(pool), slog.New(slog.DiscardHandler)))
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, req *http.Request) (int, string) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func post(t *testing.T, target string, form url.Values) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return do(t, req)
}

var secretRe = regexp.MustCompile(`whsec_[A-Za-z0-9+/=]+`)

func storedSecret(t *testing.T) []byte {
	t.Helper()
	var s []byte
	if err := pool.QueryRow(context.Background(), "SELECT secret FROM endpoints").Scan(&s); err != nil {
		t.Fatalf("read secret: %v", err)
	}
	return s
}

// The secret appears once, in the response that created or rotated it, and
// matches what the worker will sign with. The page itself never shows it.
func TestCreateAndRotateShowSecretOnce(t *testing.T) {
	srv := newServer(t)

	code, body := post(t, srv.URL+"/dashboard/endpoints", url.Values{"url": {"https://example.com/hook"}})
	if code != http.StatusOK {
		t.Fatalf("create status = %d: %s", code, body)
	}
	created := secretRe.FindString(body)
	if raw, err := signing.DecodeSecret(created); err != nil || !bytes.Equal(raw, storedSecret(t)) {
		t.Fatalf("created secret %q does not match the stored one (err %v)", created, err)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/dashboard/", nil)
	code, page := do(t, req)
	if code != http.StatusOK || !strings.Contains(page, "https://example.com/hook") {
		t.Fatalf("page status %d, want 200 listing the endpoint: %s", code, page)
	}
	if secretRe.MatchString(page) {
		t.Error("page shows a secret; it must appear only once")
	}

	var id string
	if err := pool.QueryRow(context.Background(), "SELECT id::text FROM endpoints").Scan(&id); err != nil {
		t.Fatalf("read id: %v", err)
	}
	if !strings.Contains(page, "/dashboard/endpoints/"+id+"/rotate") {
		t.Errorf("page has no rotate button for %s", id)
	}

	code, body = post(t, srv.URL+"/dashboard/endpoints/"+id+"/rotate", nil)
	if code != http.StatusOK {
		t.Fatalf("rotate status = %d: %s", code, body)
	}
	rotated := secretRe.FindString(body)
	if rotated == created {
		t.Error("rotate returned the old secret")
	}
	if raw, err := signing.DecodeSecret(rotated); err != nil || !bytes.Equal(raw, storedSecret(t)) {
		t.Errorf("rotated secret %q does not match the stored one (err %v)", rotated, err)
	}
}

func TestCreateRejectsInvalidURL(t *testing.T) {
	srv := newServer(t)
	_, body := post(t, srv.URL+"/dashboard/endpoints", url.Values{"url": {"ftp://example.com"}})
	if !strings.Contains(body, `class="error"`) {
		t.Errorf("no error shown: %s", body)
	}
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM endpoints").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("endpoints = %d, want 0", n)
	}
}

// Another site must not be able to rotate a secret through a viewer's browser.
func TestCrossOriginPostIsRefused(t *testing.T) {
	srv := newServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/dashboard/endpoints",
		strings.NewReader(url.Values{"url": {"https://example.com/hook"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	if code, _ := do(t, req); code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", code)
	}
}

func TestRotateUnknownEndpoint(t *testing.T) {
	srv := newServer(t)
	_, body := post(t, srv.URL+"/dashboard/endpoints/01900000-0000-7000-8000-000000000000/rotate", nil)
	if !strings.Contains(body, "Endpoint not found.") {
		t.Errorf("no not-found error: %s", body)
	}
}
