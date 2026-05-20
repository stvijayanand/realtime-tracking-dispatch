package handler

import (
	"encoding/json"
	"net/http"
)

// HealthHandler handles GET /health.
// Returns 200 {"status": "ok"} to indicate the service is running.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}
