package events

import "fmt"

// TripAssignedEvent is the parsed domain event for a driver assignment.
// All fields are unexported — access via accessor methods only.
// ParseTripAssigned never returns a partially-constructed TripAssignedEvent.
type TripAssignedEvent struct {
	eventID    string
	eventType  string
	tripID     string
	driverID   string
	riderID    string
	assignedAt string
}

// Accessors
func (e TripAssignedEvent) EventID() string    { return e.eventID }
func (e TripAssignedEvent) EventType() string  { return e.eventType }
func (e TripAssignedEvent) TripID() string     { return e.tripID }
func (e TripAssignedEvent) DriverID() string   { return e.driverID }
func (e TripAssignedEvent) RiderID() string    { return e.riderID }
func (e TripAssignedEvent) AssignedAt() string { return e.assignedAt }

// EventParseError is returned when a required field is absent or empty.
// Field identifies which field failed validation.
type EventParseError struct {
	Field   string
	Message string
}

func (e *EventParseError) Error() string {
	return fmt.Sprintf("event parse error: field %q: %s", e.Field, e.Message)
}

// ParseTripAssigned parses a TripAssigned envelope map into a TripAssignedEvent.
//
// The envelope map has top-level keys: event_id, event_type, occurred_at, payload.
// The payload map has keys: trip_id, driver_id, rider_id, assigned_at.
//
// Returns EventParseError identifying the missing field if any required field is absent.
// Never returns a partially-constructed TripAssignedEvent — on error the zero value is returned.
func ParseTripAssigned(envelope map[string]interface{}) (TripAssignedEvent, error) {
	eventID, err := requireString(envelope, "event_id")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	eventType, err := requireString(envelope, "event_type")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	rawPayload, ok := envelope["payload"]
	if !ok || rawPayload == nil {
		return TripAssignedEvent{}, &EventParseError{Field: "payload", Message: "required field is absent"}
	}
	payload, ok := rawPayload.(map[string]interface{})
	if !ok {
		return TripAssignedEvent{}, &EventParseError{Field: "payload", Message: "must be a non-nil map"}
	}

	tripID, err := requireString(payload, "trip_id")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	driverID, err := requireString(payload, "driver_id")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	riderID, err := requireString(payload, "rider_id")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	assignedAt, err := requireString(payload, "assigned_at")
	if err != nil {
		return TripAssignedEvent{}, err
	}

	return TripAssignedEvent{
		eventID:    eventID,
		eventType:  eventType,
		tripID:     tripID,
		driverID:   driverID,
		riderID:    riderID,
		assignedAt: assignedAt,
	}, nil
}

// requireString extracts a non-empty string value from a map by key.
// Returns EventParseError if the key is absent or the value is not a non-empty string.
func requireString(m map[string]interface{}, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", &EventParseError{Field: key, Message: "required field is absent"}
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", &EventParseError{Field: key, Message: "must be a non-empty string"}
	}
	return s, nil
}
