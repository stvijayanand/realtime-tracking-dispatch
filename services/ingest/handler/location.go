package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/realtime-tracking/ingest/events"
	"github.com/realtime-tracking/ingest/kafka"
	"github.com/realtime-tracking/ingest/model"
)

// Publisher is the interface satisfied by *kafka.Producer.
// Defined here so tests can inject a mock without importing the kafka package.
type Publisher interface {
	Publish(ctx context.Context, key string, event events.DomainEvent) (string, error)
}

// Ensure *kafka.Producer satisfies Publisher at compile time.
var _ Publisher = (*kafka.Producer)(nil)

// LocationHandler handles POST /location.
// Producer is injected at construction time — never instantiated inside ServeHTTP.
// Validator is injected at construction time for testability and reuse.
type LocationHandler struct {
	Producer  Publisher
	Validator *validator.Validate
}

// ServeHTTP implements http.Handler for POST /location.
//
// Flow:
//  1. Decode JSON body — check for *http.MaxBytesError (413) before other errors.
//  2. Validate struct tags — return 422 with structured error body on failure.
//  3. Build domain event via BuildLocationPingEvent.
//  4. Publish to Kafka — return 503 on failure.
//  5. Return 202 {"message_id": event_id} on success.
func (h *LocationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var ping model.GpsPingRequest

	// Decode JSON body. Check for MaxBytesError (body too large) first.
	if err := json.NewDecoder(r.Body).Decode(&ping); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
			return
		}
		writeJSON(w, http.StatusUnprocessableEntity, map[string]interface{}{
			"detail": []map[string]string{{"field": "body", "message": "invalid JSON"}},
		})
		return
	}

	// Validate struct tags (required, min, max, etc.).
	if err := h.Validator.Struct(ping); err != nil {
		var ve validator.ValidationErrors
		if errors.As(err, &ve) {
			detail := make([]map[string]string, 0, len(ve))
			for _, fe := range ve {
				detail = append(detail, map[string]string{
					"field":   fe.Field(),
					"message": fe.Tag(),
				})
			}
			writeJSON(w, http.StatusUnprocessableEntity, map[string]interface{}{"detail": detail})
			return
		}
		// Fallback for non-ValidationErrors (should not occur in practice).
		writeJSON(w, http.StatusUnprocessableEntity, map[string]interface{}{
			"detail": []map[string]string{{"field": "unknown", "message": err.Error()}},
		})
		return
	}

	// Build the domain event envelope.
	event := events.BuildLocationPingEvent(ping)

	// Publish to Kafka. Pass request context so OTel trace propagation works.
	eventID, err := h.Producer.Publish(r.Context(), ping.DriverID, event)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "service unavailable"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"message_id": eventID})
}

// writeJSON writes a JSON-encoded body with the given HTTP status code.
// Sets Content-Type to application/json before writing the header.
func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body) //nolint:errcheck
}
