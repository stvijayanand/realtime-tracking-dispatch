package model

// GpsPingRequest is the JSON body for POST /location.
// Validation tags enforce the API contract (Requirement 2.1, 2.4, 2.5, 10.9).
type GpsPingRequest struct {
	// DriverID identifies the driver emitting this GPS ping.
	// Must be non-empty and at most 128 characters.
	DriverID string `json:"driver_id" validate:"required,max=128"`

	// Latitude is the WGS-84 latitude of the driver's position.
	// Valid range: -90.0 to 90.0 (inclusive).
	Latitude float64 `json:"latitude" validate:"required,min=-90,max=90"`

	// Longitude is the WGS-84 longitude of the driver's position.
	// Valid range: -180.0 to 180.0 (inclusive).
	Longitude float64 `json:"longitude" validate:"required,min=-180,max=180"`

	// Timestamp is the ISO 8601 UTC timestamp from the driver's device
	// recording when the GPS ping was captured. Provided by the client —
	// not generated server-side.
	Timestamp string `json:"timestamp" validate:"required"`
}
