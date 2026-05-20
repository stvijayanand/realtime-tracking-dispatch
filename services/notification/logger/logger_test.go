package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/realtime-tracking/notification/events"
	"github.com/realtime-tracking/notification/logger"
)

// buildTestEvent creates a TripAssignedEvent via ParseTripAssigned for use in tests.
func buildTestEvent(t *testing.T) events.TripAssignedEvent {
	t.Helper()
	env := map[string]interface{}{
		"event_id":    "550e8400-e29b-41d4-a716-446655440000",
		"event_type":  "TripAssigned",
		"occurred_at": "2024-01-15T10:30:00Z",
		"payload": map[string]interface{}{
			"trip_id":     "660e8400-e29b-41d4-a716-446655440001",
			"driver_id":   "driver-001",
			"rider_id":    "rider-abc",
			"assigned_at": "2024-01-15T10:30:01Z",
		},
	}
	e, err := events.ParseTripAssigned(env)
	if err != nil {
		t.Fatalf("failed to build test event: %v", err)
	}
	return e
}

func TestLogNotification_WritesToStdout(t *testing.T) {
	var stdout, stderr bytes.Buffer
	l := logger.NewWithWriters(&stdout, &stderr)

	l.LogNotification(context.Background(), buildTestEvent(t))

	if stdout.Len() == 0 {
		t.Error("expected output on stdout, got nothing")
	}
	if stderr.Len() != 0 {
		t.Errorf("expected no output on stderr, got: %s", stderr.String())
	}
}

func TestLogNotification_ContainsAllRequiredFields(t *testing.T) {
	var stdout bytes.Buffer
	l := logger.NewWithWriters(&stdout, &bytes.Buffer{})

	l.LogNotification(context.Background(), buildTestEvent(t))

	var record map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &record); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, stdout.String())
	}

	requiredFields := []string{
		"event_id", "event_type", "trip_id", "driver_id",
		"rider_id", "assigned_at", "notification_sent_at",
	}
	for _, field := range requiredFields {
		if v, ok := record[field]; !ok || v == "" {
			t.Errorf("required field %q missing or empty in log output", field)
		}
	}
}

func TestLogNotification_FieldValuesMatchEvent(t *testing.T) {
	var stdout bytes.Buffer
	l := logger.NewWithWriters(&stdout, &bytes.Buffer{})
	event := buildTestEvent(t)

	l.LogNotification(context.Background(), event)

	var record map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &record); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	checks := map[string]string{
		"event_id":  event.EventID(),
		"event_type": event.EventType(),
		"trip_id":   event.TripID(),
		"driver_id": event.DriverID(),
		"rider_id":  event.RiderID(),
		"assigned_at": event.AssignedAt(),
	}
	for field, want := range checks {
		got, _ := record[field].(string)
		if got != want {
			t.Errorf("field %q: got %q, want %q", field, got, want)
		}
	}
}

func TestLogWarning_WritesToStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	l := logger.NewWithWriters(&stdout, &stderr)

	l.LogWarning(context.Background(), "test warning message", []byte{0xDE, 0xAD, 0xBE, 0xEF})

	if stderr.Len() == 0 {
		t.Error("expected output on stderr, got nothing")
	}
	if stdout.Len() != 0 {
		t.Errorf("expected no output on stdout for warning, got: %s", stdout.String())
	}

	var record map[string]interface{}
	if err := json.Unmarshal(stderr.Bytes(), &record); err != nil {
		t.Fatalf("stderr output is not valid JSON: %v\noutput: %s", err, stderr.String())
	}
	if record["raw_bytes_hex"] == "" {
		t.Error("expected raw_bytes_hex in warning log")
	}
}
