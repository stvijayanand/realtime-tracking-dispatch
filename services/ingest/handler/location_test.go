package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/realtime-tracking/ingest/events"
	"github.com/realtime-tracking/ingest/handler"
	"github.com/realtime-tracking/ingest/middleware"
)

// mockPublisher is a test double for the Kafka producer.
// PublishFn controls the behaviour of each call.
type mockPublisher struct {
	PublishFn func(ctx context.Context, key string, event events.DomainEvent) (string, error)
	// Calls records every invocation for assertion.
	Calls []publishCall
}

type publishCall struct {
	Key   string
	Event events.DomainEvent
}

func (m *mockPublisher) Publish(ctx context.Context, key string, event events.DomainEvent) (string, error) {
	m.Calls = append(m.Calls, publishCall{Key: key, Event: event})
	if m.PublishFn != nil {
		return m.PublishFn(ctx, key, event)
	}
	return event.EventID, nil
}

// newHandler returns a LocationHandler wired with the given mock producer.
func newHandler(mock *mockPublisher) *handler.LocationHandler {
	return &handler.LocationHandler{
		Producer:  mock,
		Validator: validator.New(),
	}
}

// validBody returns a JSON body that passes all validation rules.
func validBody() []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"driver_id": "driver-001",
		"latitude":  51.5074,
		"longitude": -0.1278,
		"timestamp": "2024-01-15T10:30:00Z",
	})
	return b
}

// TestLocationHandler_ValidRequest_Returns202 verifies that a valid request
// results in HTTP 202 with a message_id in the response body.
func TestLocationHandler_ValidRequest_Returns202(t *testing.T) {
	mock := &mockPublisher{}
	h := newHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/location", bytes.NewReader(validBody()))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if resp["message_id"] == "" {
		t.Error("expected non-empty message_id in response")
	}

	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 Publish call, got %d", len(mock.Calls))
	}
	if mock.Calls[0].Key != "driver-001" {
		t.Errorf("expected Kafka key 'driver-001', got %q", mock.Calls[0].Key)
	}
}

// TestLocationHandler_InvalidJSON_Returns422 verifies that malformed JSON
// results in HTTP 422 with a structured error body.
func TestLocationHandler_InvalidJSON_Returns422(t *testing.T) {
	mock := &mockPublisher{}
	h := newHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/location", strings.NewReader("{not valid json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d; body: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if _, ok := resp["detail"]; !ok {
		t.Error("expected 'detail' key in 422 response body")
	}

	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 Publish calls on invalid JSON, got %d", len(mock.Calls))
	}
}

// TestLocationHandler_MissingDriverID_Returns422 verifies that a request
// missing driver_id results in HTTP 422 with a field error for DriverID.
func TestLocationHandler_MissingDriverID_Returns422(t *testing.T) {
	mock := &mockPublisher{}
	h := newHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{
		"latitude":  51.5074,
		"longitude": -0.1278,
		"timestamp": "2024-01-15T10:30:00Z",
		// driver_id intentionally omitted
	})

	req := httptest.NewRequest(http.MethodPost, "/location", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d; body: %s", rr.Code, rr.Body.String())
	}

	assertDetailContainsField(t, rr.Body.Bytes(), "DriverID")

	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 Publish calls on missing DriverID, got %d", len(mock.Calls))
	}
}

// TestLocationHandler_InvalidLatitude_Returns422 verifies that a latitude
// outside [-90, 90] results in HTTP 422 with a field error for Latitude.
func TestLocationHandler_InvalidLatitude_Returns422(t *testing.T) {
	mock := &mockPublisher{}
	h := newHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{
		"driver_id": "driver-001",
		"latitude":  91.0, // out of range
		"longitude": -0.1278,
		"timestamp": "2024-01-15T10:30:00Z",
	})

	req := httptest.NewRequest(http.MethodPost, "/location", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d; body: %s", rr.Code, rr.Body.String())
	}

	assertDetailContainsField(t, rr.Body.Bytes(), "Latitude")

	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 Publish calls on invalid Latitude, got %d", len(mock.Calls))
	}
}

// TestLocationHandler_KafkaError_Returns503 verifies that a Kafka publish
// failure results in HTTP 503 with an error body.
func TestLocationHandler_KafkaError_Returns503(t *testing.T) {
	mock := &mockPublisher{
		PublishFn: func(_ context.Context, _ string, _ events.DomainEvent) (string, error) {
			return "", fmt.Errorf("kafka broker unavailable")
		},
	}
	h := newHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/location", bytes.NewReader(validBody()))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d; body: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if resp["error"] == "" {
		t.Error("expected non-empty 'error' field in 503 response body")
	}
}

// TestLocationHandler_OversizedBody_Returns413 verifies that a request body
// exceeding 65536 bytes results in HTTP 413.
// The MaxBodySize middleware must be applied before the handler for this to work.
func TestLocationHandler_OversizedBody_Returns413(t *testing.T) {
	mock := &mockPublisher{}
	h := newHandler(mock)

	// Wrap the handler with the MaxBodySize middleware (limit = 65536 bytes).
	wrapped := middleware.MaxBodySize(65536)(h)

	// Build a body that exceeds 64 KB. The JSON padding ensures the body is
	// syntactically valid up to the limit but the total size exceeds 65536 bytes.
	// We use a large driver_id padded to push the body over the limit.
	padding := strings.Repeat("x", 65537)
	body, _ := json.Marshal(map[string]interface{}{
		"driver_id": padding,
		"latitude":  51.5074,
		"longitude": -0.1278,
		"timestamp": "2024-01-15T10:30:00Z",
	})

	req := httptest.NewRequest(http.MethodPost, "/location", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d; body: %s", rr.Code, rr.Body.String())
	}

	if len(mock.Calls) != 0 {
		t.Errorf("expected 0 Publish calls on oversized body, got %d", len(mock.Calls))
	}
}

// assertDetailContainsField checks that the 422 response body contains a
// "detail" array with at least one entry whose "field" matches fieldName.
func assertDetailContainsField(t *testing.T, body []byte, fieldName string) {
	t.Helper()

	var resp struct {
		Detail []map[string]string `json:"detail"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed to decode 422 body: %v", err)
	}
	for _, entry := range resp.Detail {
		if entry["field"] == fieldName {
			return
		}
	}
	t.Errorf("expected detail entry with field=%q, got: %v", fieldName, resp.Detail)
}
