package middleware

import (
	"net/http"
)

// MaxBodySize returns middleware that limits the request body to limit bytes.
// It wraps the request body with http.MaxBytesReader so the limit is enforced
// during body reading. The handler is responsible for detecting *http.MaxBytesError
// and returning HTTP 413 — this middleware sets up the limit before the handler runs.
func MaxBodySize(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}
