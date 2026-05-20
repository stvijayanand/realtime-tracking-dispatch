// Package envelope defines the standard Kafka message envelope for all Domain Events
// in the realtime-tracking-dispatch platform.
//
// This package is infrastructure-only. Domain types (Trip, DriverLocation, Notification)
// MUST NOT be placed here. Only envelope structure and validation belong in this package.
package envelope

import (
	"fmt"

	"github.com/google/uuid"
)

// DomainEventEnvelope is the standard Kafka message envelope for all Domain Events.
// All fields use avro struct tags for Avro serialisation via confluent-kafka-go.
type DomainEventEnvelope struct {
	EventID    string                 `avro:"event_id"`
	EventType  string                 `avro:"event_type"`
	OccurredAt string                 `avro:"occurred_at"`
	Payload    map[string]interface{} `avro:"payload"`
}

// EnvelopeValidationError is returned when envelope validation fails.
// Field identifies which field failed validation.
type EnvelopeValidationError struct {
	Field   string
	Message string
}

func (e *EnvelopeValidationError) Error() string {
	return fmt.Sprintf("envelope validation failed: field %q: %s", e.Field, e.Message)
}

// Validate checks that the envelope has a non-empty UUID EventID and non-empty EventType.
// Returns an *EnvelopeValidationError identifying the failing field, or nil if valid.
// EventID is validated first; if it fails, EventType is not checked.
func Validate(e DomainEventEnvelope) error {
	if e.EventID == "" {
		return &EnvelopeValidationError{
			Field:   "EventID",
			Message: "must be a non-empty UUID",
		}
	}

	if _, err := uuid.Parse(e.EventID); err != nil {
		return &EnvelopeValidationError{
			Field:   "EventID",
			Message: fmt.Sprintf("must be a valid UUID: %s", err.Error()),
		}
	}

	if e.EventType == "" {
		return &EnvelopeValidationError{
			Field:   "EventType",
			Message: "must be non-empty",
		}
	}

	return nil
}
