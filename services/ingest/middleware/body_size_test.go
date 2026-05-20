package middleware_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/realtime-tracking/ingest/middleware"
)

const testLimit int64 = 65536 // 64 KB

// echoHandler reads the full request body and echoes it back with 200 OK.
// It is used to verify that bodies within the limit reach the handler.
var echoHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		// If MaxBytesReader triggered, ReadAll returns an error.
		// The handler must detect *http.MaxBytesError and return 413.
		var maxBytesErr *http.MaxBytesError
		if isMaxBytesError(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(body) //nolint:errcheck
})

// isMaxBytesError checks whether err is (or wraps) *http.MaxBytesError.
func isMaxBytesError(err error, target **http.MaxBytesError) bool {
	// errors.As is not available without importing errors; use type assertion.
	if mbe, ok := err.(*http.MaxBytesError); ok {
		*target = mbe
		return true
	}
	return false
}

// TestMaxBodySize_BodyAtLimit verifies that a body exactly at the limit (65536
// bytes) passes through to the handler and receives HTTP 200.
func TestMaxBodySize_BodyAtLimit(t *testing.T) {
	wrapped := middleware.MaxBodySize(testLimit)(echoHandler)

	body := bytes.Repeat([]byte("a"), int(testLimit))
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for body at limit, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// TestMaxBodySize_BodyBelowLimit verifies that a body smaller than the limit
// passes through to the handler and receives HTTP 200.
func TestMaxBodySize_BodyBelowLimit(t *testing.T) {
	wrapped := middleware.MaxBodySize(testLimit)(echoHandler)

	body := []byte("hello world")
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for small body, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// TestMaxBodySize_BodyExceedsLimit verifies that a body exceeding the limit
// results in HTTP 413 when the handler checks for *http.MaxBytesError.
func TestMaxBodySize_BodyExceedsLimit(t *testing.T) {
	wrapped := middleware.MaxBodySize(testLimit)(echoHandler)

	// One byte over the limit.
	body := bytes.Repeat([]byte("a"), int(testLimit)+1)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for body exceeding limit, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// TestMaxBodySize_LargeBodyExceedsLimit verifies that a significantly oversized
// body (128 KB) also results in HTTP 413.
func TestMaxBodySize_LargeBodyExceedsLimit(t *testing.T) {
	wrapped := middleware.MaxBodySize(testLimit)(echoHandler)

	body := strings.Repeat("x", 128*1024) // 128 KB
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for 128 KB body, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// TestMaxBodySize_EmptyBody verifies that an empty body passes through to the
// handler and receives HTTP 200.
func TestMaxBodySize_EmptyBody(t *testing.T) {
	wrapped := middleware.MaxBodySize(testLimit)(echoHandler)

	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for empty body, got %d; body: %s", rr.Code, rr.Body.String())
	}
}
