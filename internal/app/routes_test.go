package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The dashboard loads htmx from the binary itself; a broken embed or route
// would leave every button dead without any server error.
func TestRoutesServeStaticHTMX(t *testing.T) {
	srv := httptest.NewServer(routes(nil, nil, slog.New(slog.DiscardHandler)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/static/htmx.min.js")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "htmx") {
		t.Errorf("status %d, %d bytes; want htmx served", resp.StatusCode, len(b))
	}
}
