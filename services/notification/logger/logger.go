// Package logger provides structured JSON logging for the Notification Service.
// All log output goes through Logger methods — never fmt.Println or raw log calls.
package logger

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/realtime-tracking/notification/events"
)

// Logger wraps log/slog and emits structured JSON log lines.
// stdout receives notification log lines; stderr receives warnings.
type Logger struct {
	out  *slog.Logger
	warn *slog.Logger
}

// New creates a Logger that writes JSON to stdout (notifications) and stderr (warnings).
func New() *Logger {
	return &Logger{
		out:  slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		warn: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

// NewWithWriters creates a Logger with custom writers — used in tests to capture output.
func NewWithWriters(stdout, stderr io.Writer) *Logger {
	return &Logger{
		out:  slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		warn: slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

// LogNotification writes a structured JSON notification log line to stdout.
// Contains all required fields per Requirement 4.2 and 12.3:
// event_id, event_type, trip_id, driver_id, rider_id, assigned_at,
// notification_sent_at, trace_id.
func (l *Logger) LogNotification(ctx context.Context, event events.TripAssignedEvent) {
	l.out.InfoContext(ctx, "notification_sent",
		slog.String("event_id", event.EventID()),
		slog.String("event_type", event.EventType()),
		slog.String("trip_id", event.TripID()),
		slog.String("driver_id", event.DriverID()),
		slog.String("rider_id", event.RiderID()),
		slog.String("assigned_at", event.AssignedAt()),
		slog.String("notification_sent_at", time.Now().UTC().Format(time.RFC3339Nano)),
		slog.String("trace_id", extractTraceID(ctx)),
	)
}

// LogWarning writes a JSON warning line to stderr with trace_id.
// Used when a Kafka message cannot be deserialised (Requirement 4.4).
func (l *Logger) LogWarning(ctx context.Context, msg string, rawBytes []byte) {
	l.warn.WarnContext(ctx, msg,
		slog.String("raw_bytes_hex", hex.EncodeToString(rawBytes)),
		slog.String("trace_id", extractTraceID(ctx)),
	)
}

// extractTraceID extracts the W3C trace ID from the OTel span context.
// Returns an empty string if no active span is present.
func extractTraceID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}
