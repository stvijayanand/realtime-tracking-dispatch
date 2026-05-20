package events

import (
	"time"

	"github.com/google/uuid"
	"github.com/realtime-tracking/ingest/model"
)

// DomainEvent is the standard Kafka message envelope for all Domain Events
// published by the Ingest Service. It maps directly to the Avro schema at
// shared/avro/location_ping_received.avsc.
type DomainEvent struct {
	// EventID is a UUID v4 uniquely identifying this event instance.
	// Generated at publish time. Used as the deduplication key for
	// consumer-side idempotency (Requirement 2.2).
	EventID string

	// EventType is the discriminator field identifying the Domain Event type.
	// Always "LocationPingReceived" for events built by BuildLocationPingEvent.
	EventType string

	// OccurredAt is the ISO 8601 UTC timestamp recording when this event
	// occurred. Set by the producer at publish time using time.Now().UTC().
	OccurredAt string

	// Payload holds the domain-specific fields for the event. Typed as
	// map[string]interface{} to support Avro serialisation via the Schema
	// Registry client (Requirement 2.2).
	Payload map[string]interface{}
}

// BuildLocationPingEvent constructs a LocationPingReceived DomainEvent from a
// validated GpsPingRequest. It generates EventID (UUID v4) and OccurredAt
// (time.Now().UTC() formatted as RFC3339Nano / ISO 8601). All four ping fields
// are copied into Payload with their canonical JSON key names to match the Avro
// schema contract. This function never returns an error — all inputs are
// pre-validated by the HTTP handler before this function is called.
func BuildLocationPingEvent(ping model.GpsPingRequest) DomainEvent {
	return DomainEvent{
		EventID:    uuid.New().String(),
		EventType:  "LocationPingReceived",
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
		Payload: map[string]interface{}{
			"driver_id": ping.DriverID,
			"latitude":  ping.Latitude,
			"longitude": ping.Longitude,
			"timestamp": ping.Timestamp,
		},
	}
}
