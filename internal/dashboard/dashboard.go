// Package dashboard serves the operator UI: server-rendered HTML that htmx
// swaps in place, so there is no client-side state or build step.
package dashboard

import (
	"embed"
	"html/template"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgtype"

	"forflow/internal/endpoint"
	"forflow/internal/signing"
	"forflow/internal/store/sqlc"
)

//go:embed templates/*.html
var templates embed.FS

var tmpl = template.Must(template.ParseFS(templates, "templates/*.html"))

type handler struct {
	queries *sqlc.Queries
	log     *slog.Logger
}

// view is what the endpoints section renders. Secret is set only on the
// response that created or rotated it; it is never stored for display later.
type view struct {
	Endpoints []sqlc.ListEndpointsRow
	Notice    string
	Secret    string
	Error     string
}

// New returns the dashboard routes under /dashboard/. Cross-origin POSTs are
// refused, so another site cannot drive the buttons from a viewer's browser.
func New(q *sqlc.Queries, log *slog.Logger) http.Handler {
	h := &handler{queries: q, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /dashboard/{$}", h.page)
	mux.HandleFunc("POST /dashboard/endpoints", h.createEndpoint)
	mux.HandleFunc("POST /dashboard/endpoints/{id}/rotate", h.rotateSecret)
	return http.NewCrossOriginProtection().Handler(mux)
}

func (h *handler) page(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, "page", view{})
}

func (h *handler) createEndpoint(w http.ResponseWriter, r *http.Request) {
	url := r.FormValue("url")
	if !endpoint.ValidURL(url) {
		h.render(w, r, "endpoints", view{Error: "URL must be an absolute http or https URL."})
		return
	}

	secret := signing.GenerateSecret()
	if _, err := h.queries.CreateEndpoint(r.Context(), sqlc.CreateEndpointParams{Url: url, Secret: secret}); err != nil {
		h.fail(w, r, "create endpoint", err)
		return
	}
	h.render(w, r, "endpoints", view{Notice: "Endpoint created. Its signing secret:", Secret: signing.EncodeSecret(secret)})
}

func (h *handler) rotateSecret(w http.ResponseWriter, r *http.Request) {
	var id pgtype.UUID
	if err := id.Scan(r.PathValue("id")); err != nil {
		http.Error(w, "invalid endpoint id", http.StatusBadRequest)
		return
	}

	secret := signing.GenerateSecret()
	n, err := h.queries.RotateEndpointSecret(r.Context(), sqlc.RotateEndpointSecretParams{ID: id, Secret: secret})
	if err != nil {
		h.fail(w, r, "rotate endpoint secret", err)
		return
	}
	if n == 0 {
		h.render(w, r, "endpoints", view{Error: "Endpoint not found."})
		return
	}
	h.log.InfoContext(r.Context(), "endpoint secret rotated", "endpoint_id", id.String())
	h.render(w, r, "endpoints", view{
		Notice: "Secret rotated. The old one keeps signing for 24 hours. New secret:",
		Secret: signing.EncodeSecret(secret),
	})
}

// render loads the endpoint list fresh for every response, so the table never
// shows a stale row after a change.
func (h *handler) render(w http.ResponseWriter, r *http.Request, name string, v view) {
	rows, err := h.queries.ListEndpoints(r.Context())
	if err != nil {
		h.fail(w, r, "list endpoints", err)
		return
	}
	v.Endpoints = rows

	// A page that shows a secret must not be kept by the browser or a proxy.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, name, v); err != nil {
		h.log.ErrorContext(r.Context(), "render dashboard", "template", name, "err", err)
	}
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.log.ErrorContext(r.Context(), op, "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}
