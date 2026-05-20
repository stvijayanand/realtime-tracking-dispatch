// Package handler contains event handler functions for the Notification Service.
package handler

import (
	"context"

	"github.com/realtime-tracking/notification/events"
	"github.com/realtime-tracking/notification/logger"
)

// HandleTripAssigned logs a structured notification for a TripAssigned event.
// This is the primary handler — called by the consumer worker when event_type == "TripAssigned".
func HandleTripAssigned(ctx context.Context, event events.TripAssignedEvent, log *logger.Logger) {
	log.LogNotification(ctx, event)
}

// HandlerFunc is the type for event handler functions keyed by event_type.
// The envelope map contains the raw deserialized Kafka message.
type HandlerFunc func(ctx context.Context, envelope map[string]interface{})

// NoOpHandler is a HandlerFunc that acknowledges and discards the message.
// Used for all event types other than "TripAssigned" (Requirement 4.3).
func NoOpHandler(_ context.Context, _ map[string]interface{}) {
	// Intentionally empty — message is acknowledged and skipped.
}
