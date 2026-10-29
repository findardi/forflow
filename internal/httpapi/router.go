// Package httpapi exposes the public HTTP API.
package httpapi

import "net/http"

// NewRouter registers every API route.
func NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /endpoints", createEndpoint)
	mux.HandleFunc("POST /messages", createMessage)
	return mux
}

func createEndpoint(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func createMessage(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
