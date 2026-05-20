package model_test

import (
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/realtime-tracking/ingest/model"
)

// validate is a package-level validator instance, matching how the HTTP handler
// will use it (one instance, reused across requests).
var validate = validator.New()

// validPing returns a GpsPingRequest that passes all validation rules.
func validPing() model.GpsPingRequest {
	return model.GpsPingRequest{
		DriverID:  "driver-001",
		Latitude:  51.5074,
		Longitude: -0.1278,
		Timestamp: "2024-01-15T10:30:00.000Z",
	}
}

// TestGpsPingRequest_Valid verifies that a fully-populated, in-range request
// passes validation without errors.
func TestGpsPingRequest_Valid(t *testing.T) {
	if err := validate.Struct(validPing()); err != nil {
		t.Errorf("expected valid ping to pass validation, got: %v", err)
	}
}

// TestGpsPingRequest_MissingDriverID verifies that an empty DriverID triggers a
// validation error on the "required" tag.
func TestGpsPingRequest_MissingDriverID(t *testing.T) {
	ping := validPing()
	ping.DriverID = ""

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for missing DriverID, got nil")
	}
	assertFieldError(t, err, "DriverID")
}

// TestGpsPingRequest_DriverIDTooLong verifies that a DriverID exceeding 128
// characters triggers a validation error on the "max" tag.
func TestGpsPingRequest_DriverIDTooLong(t *testing.T) {
	ping := validPing()
	ping.DriverID = strings.Repeat("x", 129)

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for DriverID > 128 chars, got nil")
	}
	assertFieldError(t, err, "DriverID")
}

// TestGpsPingRequest_DriverIDAtMaxLength verifies that a DriverID of exactly
// 128 characters passes validation (boundary value).
func TestGpsPingRequest_DriverIDAtMaxLength(t *testing.T) {
	ping := validPing()
	ping.DriverID = strings.Repeat("x", 128)

	if err := validate.Struct(ping); err != nil {
		t.Errorf("expected DriverID of 128 chars to pass validation, got: %v", err)
	}
}

// TestGpsPingRequest_LatitudeTooLow verifies that a Latitude below -90 triggers
// a validation error on the "min" tag.
func TestGpsPingRequest_LatitudeTooLow(t *testing.T) {
	ping := validPing()
	ping.Latitude = -90.0001

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for Latitude < -90, got nil")
	}
	assertFieldError(t, err, "Latitude")
}

// TestGpsPingRequest_LatitudeTooHigh verifies that a Latitude above 90 triggers
// a validation error on the "max" tag.
func TestGpsPingRequest_LatitudeTooHigh(t *testing.T) {
	ping := validPing()
	ping.Latitude = 90.0001

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for Latitude > 90, got nil")
	}
	assertFieldError(t, err, "Latitude")
}

// TestGpsPingRequest_LatitudeAtBoundaries verifies that the boundary values
// -90 and 90 are accepted (inclusive range).
func TestGpsPingRequest_LatitudeAtBoundaries(t *testing.T) {
	for _, lat := range []float64{-90.0, 90.0} {
		ping := validPing()
		ping.Latitude = lat
		if err := validate.Struct(ping); err != nil {
			t.Errorf("expected Latitude %v to pass validation, got: %v", lat, err)
		}
	}
}

// TestGpsPingRequest_LongitudeTooLow verifies that a Longitude below -180
// triggers a validation error on the "min" tag.
func TestGpsPingRequest_LongitudeTooLow(t *testing.T) {
	ping := validPing()
	ping.Longitude = -180.0001

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for Longitude < -180, got nil")
	}
	assertFieldError(t, err, "Longitude")
}

// TestGpsPingRequest_LongitudeTooHigh verifies that a Longitude above 180
// triggers a validation error on the "max" tag.
func TestGpsPingRequest_LongitudeTooHigh(t *testing.T) {
	ping := validPing()
	ping.Longitude = 180.0001

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for Longitude > 180, got nil")
	}
	assertFieldError(t, err, "Longitude")
}

// TestGpsPingRequest_LongitudeAtBoundaries verifies that the boundary values
// -180 and 180 are accepted (inclusive range).
func TestGpsPingRequest_LongitudeAtBoundaries(t *testing.T) {
	for _, lng := range []float64{-180.0, 180.0} {
		ping := validPing()
		ping.Longitude = lng
		if err := validate.Struct(ping); err != nil {
			t.Errorf("expected Longitude %v to pass validation, got: %v", lng, err)
		}
	}
}

// TestGpsPingRequest_MissingTimestamp verifies that an empty Timestamp triggers
// a validation error on the "required" tag.
func TestGpsPingRequest_MissingTimestamp(t *testing.T) {
	ping := validPing()
	ping.Timestamp = ""

	err := validate.Struct(ping)
	if err == nil {
		t.Fatal("expected validation error for missing Timestamp, got nil")
	}
	assertFieldError(t, err, "Timestamp")
}

// assertFieldError is a test helper that fails the test if the validation error
// does not contain a field error for the named struct field.
func assertFieldError(t *testing.T, err error, fieldName string) {
	t.Helper()

	var ve validator.ValidationErrors
	var ok bool
	if ve, ok = err.(validator.ValidationErrors); !ok {
		t.Fatalf("expected validator.ValidationErrors, got %T: %v", err, err)
	}

	for _, fe := range ve {
		if fe.Field() == fieldName {
			return // found the expected field error
		}
	}

	t.Errorf("expected validation error for field %q, but it was not present in: %v", fieldName, ve)
}
