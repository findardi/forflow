//go:build integration

package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/httpapi"
	"forflow/internal/message"
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
	srv := httptest.NewServer(httpapi.NewRouter(sqlc.New(pool), message.NewService(pool), slog.New(slog.DiscardHandler)))
	t.Cleanup(srv.Close)
	return srv
}

// The list shows each endpoint and its health but never a secret, which is
// shown only once, at creation or rotation.
func TestListEndpointsHidesSecrets(t *testing.T) {
	srv := newServer(t)
	resp, err := http.Post(srv.URL+"/endpoints", "application/json", strings.NewReader(`{"url":"https://example.com/hook"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var created struct{ ID, Secret string }
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	_ = resp.Body.Close()

	resp, err = http.Get(srv.URL + "/endpoints")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}

	var list struct {
		Data []struct {
			ID                  string  `json:"id"`
			URL                 string  `json:"url"`
			ConsecutiveFailures int32   `json:"consecutive_failures"`
			DisabledAt          *string `json:"disabled_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Data) != 1 {
		t.Fatalf("got %d endpoints, want 1: %s", len(list.Data), body)
	}
	if e := list.Data[0]; e.ID != created.ID || e.URL != "https://example.com/hook" || e.DisabledAt != nil {
		t.Errorf("endpoint = %+v, want id %s, its url, not disabled", e, created.ID)
	}
	if strings.Contains(string(body), "secret") || strings.Contains(string(body), created.Secret) {
		t.Errorf("list leaks a secret: %s", body)
	}
}
