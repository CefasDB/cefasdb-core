package httpx

import (
	"encoding/json"
	"net/http"
)

// WriteJSON encodes body as JSON with the given status code. It sets
// Content-Type and ignores encoding errors — a failed write is
// observable via the standard http.Server logs, and the response
// status has already been committed.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// WriteErr writes a one-field JSON error body ({"error": "..."}).
// status is the HTTP status code the caller already mapped from the
// domain error.
func WriteErr(w http.ResponseWriter, status int, err error) {
	WriteJSON(w, status, map[string]string{"error": err.Error()})
}
