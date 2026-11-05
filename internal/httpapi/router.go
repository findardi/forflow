// Package httpapi exposes the public HTTP API.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/jackc/pgx/v5/pgtype"

	"forflow/internal/message"
	"forflow/internal/signing"
	"forflow/internal/store/sqlc"
)

const (
	maxEndpointBody   = 4 << 10
	maxMessageBody    = 1 << 20
	maxIdempotencyKey = 255
)

type api struct {
	queries  *sqlc.Queries
	messages *message.Service
	log      *slog.Logger
}

// NewRouter registers every API route.
func NewRouter(q *sqlc.Queries, msg *message.Service, log *slog.Logger) http.Handler {
	a := &api{
		queries:  q,
		messages: msg,
		log:      log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /endpoints", a.createEndpoint)
	mux.HandleFunc("POST /endpoints/{id}/enable", a.enableEndpoint)
	mux.HandleFunc("POST /endpoints/{id}/secret/rotate", a.rotateSecret)
	mux.HandleFunc("POST /messages", a.createMessage)
	return mux
}

func (a *api) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxEndpointBody)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if !validURL(req.URL) {
		http.Error(w, "url must be an absolute http or https URL", http.StatusBadRequest)
		return
	}

	ep, err := a.queries.CreateEndpoint(r.Context(), sqlc.CreateEndpointParams{
		Url:    req.URL,
		Secret: signing.GenerateSecret(),
	})
	if err != nil {
		a.log.ErrorContext(r.Context(), "create endpoint", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// The secret is shown only here; later reads never expose it.
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         ep.ID,
		"url":        ep.Url,
		"secret":     signing.EncodeSecret(ep.Secret),
		"created_at": ep.CreatedAt,
	})
}

// enableEndpoint turns a disabled endpoint back on and clears its breaker.
// Re-enabling is a human decision, so it never happens on its own.
func (a *api) enableEndpoint(w http.ResponseWriter, r *http.Request) {
	var id pgtype.UUID
	if err := id.Scan(r.PathValue("id")); err != nil {
		http.Error(w, "invalid endpoint id", http.StatusBadRequest)
		return
	}

	n, err := a.queries.EnableEndpoint(r.Context(), id)
	if err != nil {
		a.log.ErrorContext(r.Context(), "enable endpoint", "endpoint_id", id.String(), "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if n == 0 {
		http.Error(w, "endpoint not found", http.StatusNotFound)
		return
	}

	a.log.InfoContext(r.Context(), "endpoint enabled", "endpoint_id", id.String())
	w.WriteHeader(http.StatusNoContent)
}

// rotateSecret replaces the endpoint's secret. The old one keeps signing for
// 24 hours, so the customer can switch without failed verifications. The new
// secret is shown only here, as at creation.
func (a *api) rotateSecret(w http.ResponseWriter, r *http.Request) {
	var id pgtype.UUID
	if err := id.Scan(r.PathValue("id")); err != nil {
		http.Error(w, "invalid endpoint id", http.StatusBadRequest)
		return
	}

	secret := signing.GenerateSecret()
	n, err := a.queries.RotateEndpointSecret(r.Context(), sqlc.RotateEndpointSecretParams{ID: id, Secret: secret})
	if err != nil {
		a.log.ErrorContext(r.Context(), "rotate endpoint secret", "endpoint_id", id.String(), "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if n == 0 {
		http.Error(w, "endpoint not found", http.StatusNotFound)
		return
	}

	a.log.InfoContext(r.Context(), "endpoint secret rotated", "endpoint_id", id.String())
	writeJSON(w, http.StatusOK, map[string]any{"secret": signing.EncodeSecret(secret)})
}

// createMessage treats the whole request body as the payload, so the bytes
// delivered later are exactly the bytes the producer sent. A repeat with the
// same Idempotency-Key gets the first message's response.
func (a *api) createMessage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxMessageBody)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	if !json.Valid(payload) {
		http.Error(w, "body must be valid JSON", http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKey {
		http.Error(w, "Idempotency-Key must be at most 255 bytes", http.StatusBadRequest)
		return
	}

	msg, n, err := a.messages.Create(r.Context(), payload, key)
	if errors.Is(err, message.ErrKeyReused) {
		http.Error(w, "Idempotency-Key was already used with a different body", http.StatusConflict)
		return
	}
	if err != nil {
		a.log.ErrorContext(r.Context(), "create message", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	a.log.InfoContext(r.Context(), "message accepted", "message_id", msg.ID, "deliveries", n)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":         msg.ID,
		"deliveries": n,
	})
}

// validURL rejects anything a worker could not POST to. It does not block
// private addresses; that is netguard's job at dial time.
func validURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
