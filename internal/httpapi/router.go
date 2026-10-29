// Package httpapi exposes the public HTTP API.
package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"forflow/internal/message"
	"forflow/internal/store/sqlc"
)

const (
	maxEndpointBody = 4 << 10
	maxMessageBody  = 1 << 20
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

	ep, err := a.queries.CreateEndpoint(r.Context(), req.URL)
	if err != nil {
		a.log.ErrorContext(r.Context(), "create endpoint", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         ep.ID,
		"url":        ep.Url,
		"created_at": ep.CreatedAt,
	})
}

// createMessage treats the whole request body as the payload, so the bytes
// delivered later are exactly the bytes the producer sent.
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

	msg, n, err := a.messages.Create(r.Context(), payload)
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
