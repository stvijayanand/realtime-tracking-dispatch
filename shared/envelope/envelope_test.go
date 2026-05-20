package envelope_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/realtime-tracking/shared/envelope"
)

// TestValidate_ValidEnvelope verifies that a well-formed envelope passes validation.
func TestValidate_ValidEnvelope(t *testing.T) {
	e := envelope.DomainEventEnvelope{
		EventID:    uuid.New().String(),
		EventType:  "LocationPingReceived",
		OccurredAt: "2024-01-15T10:30:00Z",
		Payload: map[string]interface{}{
			"driver_id": "driver-001",
			"latitude":  51.5074,
			"longitude": -0.1278,
		},
	}

	if err := envelope.Validate(e); err != nil {
		t.Errorf("expected no error for valid envelope, got: %v", err)
	}
}

// TestValidate_EmptyEventID verifies that an empty EventID returns an EnvelopeValidationError
// with Field set to "EventID".
func TestValidate_EmptyEventID(t *testing.T) {
	e := envelope.DomainEventEnvelope{
		EventID:   "",
		EventType: "LocationPingReceived",
	}

	err := envelope.Validate(e)
	if err == nil {
		t.Fatal("expected error for empty EventID, got nil")
	}

	var valErr *envelope.EnvelopeValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *EnvelopeValidationError, got %T: %v", err, err)
	}

	if valErr.Field != "EventID" {
		t.Errorf("expected Field=%q, got Field=%q", "EventID", valErr.Field)
	}
}

// TestValidate_InvalidUUIDEventID verifies that a non-UUID EventID returns an
// EnvelopeValidationError with Field set to "EventID".
func TestValidate_InvalidUUIDEventID(t *testing.T) {
	invalidIDs := []string{
		"not-a-uuid",
		"12345",
		"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
		"00000000-0000-0000-0000",
	}

	for _, id := range invalidIDs {
		t.Run(id, func(t *testing.T) {
			e := envelope.DomainEventEnvelope{
				EventID:   id,
				EventType: "LocationPingReceived",
			}

			err := envelope.Validate(e)
			if err == nil {
				t.Fatalf("expected error for invalid UUID %q, got nil", id)
			}

			var valErr *envelope.EnvelopeValidationError
			if !errors.As(err, &valErr) {
				t.Fatalf("expected *EnvelopeValidationError, got %T: %v", err, err)
			}

			if valErr.Field != "EventID" {
				t.Errorf("expected Field=%q, got Field=%q", "EventID", valErr.Field)
			}
		})
	}
}

// TestValidate_EmptyEventType verifies that an empty EventType returns an
// EnvelopeValidationError with Field set to "EventType".
func TestValidate_EmptyEventType(t *testing.T) {
	e := envelope.DomainEventEnvelope{
		EventID:   uuid.New().String(),
		EventType: "",
	}

	err := envelope.Validate(e)
	if err == nil {
		t.Fatal("expected error for empty EventType, got nil")
	}

	var valErr *envelope.EnvelopeValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected *EnvelopeValidationError, got %T: %v", err, err)
	}

	if valErr.Field != "EventType" {
		t.Errorf("expected Field=%q, got Field=%q", "EventType", valErr.Field)
	}
}

// TestEnvelopeValidationError_Error verifies the error message format.
func TestEnvelopeValidationError_Error(t *testing.T) {
	err := &envelope.EnvelopeValidationError{
		Field:   "EventID",
		Message: "must be a non-empty UUID",
	}

	msg := err.Error()
	if msg == "" {
		t.Error("expected non-empty error message")
	}

	// Verify the error message contains the field name and message
	expected := `envelope validation failed: field "EventID": must be a non-empty UUID`
	if msg != expected {
		t.Errorf("expected error message %q, got %q", expected, msg)
	}
}
