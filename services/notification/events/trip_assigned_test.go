package events_test

import (
	"errors"
	"testing"

	"github.com/realtime-tracking/notification/events"
)

func validEnvelope() map[string]interface{} {
	return map[string]interface{}{
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
}

func TestParseTripAssigned_ValidEnvelope(t *testing.T) {
	e, err := events.ParseTripAssigned(validEnvelope())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if e.EventID() != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("EventID = %q", e.EventID())
	}
	if e.EventType() != "TripAssigned" {
		t.Errorf("EventType = %q", e.EventType())
	}
	if e.TripID() != "660e8400-e29b-41d4-a716-446655440001" {
		t.Errorf("TripID = %q", e.TripID())
	}
	if e.DriverID() != "driver-001" {
		t.Errorf("DriverID = %q", e.DriverID())
	}
	if e.RiderID() != "rider-abc" {
		t.Errorf("RiderID = %q", e.RiderID())
	}
	if e.AssignedAt() != "2024-01-15T10:30:01Z" {
		t.Errorf("AssignedAt = %q", e.AssignedAt())
	}
}

func assertParseError(t *testing.T, envelope map[string]interface{}, wantField string) {
	t.Helper()
	_, err := events.ParseTripAssigned(envelope)
	if err == nil {
		t.Fatalf("expected error for missing field %q, got nil", wantField)
	}
	var pe *events.EventParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *EventParseError, got %T: %v", err, err)
	}
	if pe.Field != wantField {
		t.Errorf("expected Field=%q, got Field=%q", wantField, pe.Field)
	}
}

func TestParseTripAssigned_MissingEventID(t *testing.T) {
	env := validEnvelope()
	delete(env, "event_id")
	assertParseError(t, env, "event_id")
}

func TestParseTripAssigned_MissingPayload(t *testing.T) {
	env := validEnvelope()
	delete(env, "payload")
	assertParseError(t, env, "payload")
}

func TestParseTripAssigned_MissingTripID(t *testing.T) {
	env := validEnvelope()
	delete(env["payload"].(map[string]interface{}), "trip_id")
	assertParseError(t, env, "trip_id")
}

func TestParseTripAssigned_MissingDriverID(t *testing.T) {
	env := validEnvelope()
	delete(env["payload"].(map[string]interface{}), "driver_id")
	assertParseError(t, env, "driver_id")
}

func TestParseTripAssigned_MissingRiderID(t *testing.T) {
	env := validEnvelope()
	delete(env["payload"].(map[string]interface{}), "rider_id")
	assertParseError(t, env, "rider_id")
}

func TestParseTripAssigned_MissingAssignedAt(t *testing.T) {
	env := validEnvelope()
	delete(env["payload"].(map[string]interface{}), "assigned_at")
	assertParseError(t, env, "assigned_at")
}

func TestParseTripAssigned_NeverReturnsPartialEvent(t *testing.T) {
	env := validEnvelope()
	delete(env, "event_id")
	e, err := events.ParseTripAssigned(env)
	if err == nil {
		t.Fatal("expected error")
	}
	// On error, all accessor methods must return empty string (zero value)
	if e.EventID() != "" || e.TripID() != "" || e.DriverID() != "" {
		t.Error("expected zero-value TripAssignedEvent on parse error")
	}
}
