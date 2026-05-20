package events_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/realtime-tracking/ingest/events"
	"github.com/realtime-tracking/ingest/model"
)

// samplePing returns a fully-populated GpsPingRequest for use in tests.
func samplePing() model.GpsPingRequest {
	return model.GpsPingRequest{
		DriverID:  "driver-abc-123",
		Latitude:  51.5074,
		Longitude: -0.1278,
		Timestamp: "2024-01-15T10:30:00.000Z",
	}
}

// TestBuildLocationPingEvent_EventID verifies that the generated EventID is a
// valid UUID v4 string.
func TestBuildLocationPingEvent_EventID(t *testing.T) {
	event := events.BuildLocationPingEvent(samplePing())

	if event.EventID == "" {
		t.Fatal("EventID must not be empty")
	}

	parsed, err := uuid.Parse(event.EventID)
	if err != nil {
		t.Fatalf("EventID %q is not a valid UUID: %v", event.EventID, err)
	}

	// UUID v4 has version bits set to 4.
	if parsed.Version() != 4 {
		t.Errorf("expected UUID version 4, got %d", parsed.Version())
	}
}

// TestBuildLocationPingEvent_EventType verifies that EventType is always
// "LocationPingReceived".
func TestBuildLocationPingEvent_EventType(t *testing.T) {
	event := events.BuildLocationPingEvent(samplePing())

	const want = "LocationPingReceived"
	if event.EventType != want {
		t.Errorf("EventType = %q, want %q", event.EventType, want)
	}
}

// TestBuildLocationPingEvent_OccurredAt verifies that OccurredAt is a valid
// RFC3339 (ISO 8601) timestamp and is close to the current time.
func TestBuildLocationPingEvent_OccurredAt(t *testing.T) {
	before := time.Now().UTC()
	event := events.BuildLocationPingEvent(samplePing())
	after := time.Now().UTC()

	if event.OccurredAt == "" {
		t.Fatal("OccurredAt must not be empty")
	}

	parsed, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		t.Fatalf("OccurredAt %q is not a valid RFC3339 timestamp: %v", event.OccurredAt, err)
	}

	// The timestamp must fall within the window of the test execution.
	if parsed.Before(before) || parsed.After(after) {
		t.Errorf("OccurredAt %q is outside the expected window [%s, %s]",
			event.OccurredAt, before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
	}
}

// TestBuildLocationPingEvent_PayloadFields verifies that all four ping fields
// are present in Payload with the correct values and canonical JSON key names.
func TestBuildLocationPingEvent_PayloadFields(t *testing.T) {
	ping := samplePing()
	event := events.BuildLocationPingEvent(ping)

	if event.Payload == nil {
		t.Fatal("Payload must not be nil")
	}

	tests := []struct {
		key  string
		want interface{}
	}{
		{"driver_id", ping.DriverID},
		{"latitude", ping.Latitude},
		{"longitude", ping.Longitude},
		{"timestamp", ping.Timestamp},
	}

	for _, tc := range tests {
		got, ok := event.Payload[tc.key]
		if !ok {
			t.Errorf("Payload missing key %q", tc.key)
			continue
		}
		if got != tc.want {
			t.Errorf("Payload[%q] = %v (%T), want %v (%T)", tc.key, got, got, tc.want, tc.want)
		}
	}
}

// TestBuildLocationPingEvent_UniqueEventIDs verifies that two successive calls
// to BuildLocationPingEvent produce different EventIDs.
func TestBuildLocationPingEvent_UniqueEventIDs(t *testing.T) {
	ping := samplePing()
	e1 := events.BuildLocationPingEvent(ping)
	e2 := events.BuildLocationPingEvent(ping)

	if e1.EventID == e2.EventID {
		t.Errorf("expected unique EventIDs, but both calls returned %q", e1.EventID)
	}
}
