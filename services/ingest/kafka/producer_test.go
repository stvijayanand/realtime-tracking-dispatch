package kafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/realtime-tracking/ingest/config"
)

// TestNewProducer_InvalidConfig verifies that NewProducer returns an error
// when the bootstrap servers string is empty (confluent rejects it).
func TestNewProducer_InvalidConfig(t *testing.T) {
	cfg := config.Config{
		KafkaBootstrapServers: "", // intentionally invalid
		KafkaTopic:            "gps-pings",
		KafkaSASLUsername:     "user",
		KafkaSASLPassword:     "pass",
		SchemaRegistryURL:     "http://localhost:8081",
	}

	_, err := NewProducer(cfg)
	if err == nil {
		t.Fatal("expected error for empty bootstrap.servers, got nil")
	}
}

// TestInjectTraceHeaders_WithActiveSpan verifies that when an active OTel span
// is present in the context, injectTraceHeaders returns at least one header
// whose key is "traceparent".
func TestInjectTraceHeaders_WithActiveSpan(t *testing.T) {
	// Wire up a real SDK tracer and register the W3C propagator so that
	// otel.GetTextMapPropagator().Inject() actually writes traceparent.
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() {
		// Reset to no-op defaults so other tests are not affected.
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	headers := injectTraceHeaders(ctx)

	found := false
	for _, h := range headers {
		if h.Key == "traceparent" {
			found = true
			if len(h.Value) == 0 {
				t.Error("traceparent header value is empty")
			}
			break
		}
	}
	if !found {
		t.Errorf("expected 'traceparent' header, got headers: %v", headers)
	}
}

// TestInjectTraceHeaders_NoSpan verifies that injectTraceHeaders does not
// panic and returns an empty (or nil) slice when there is no active span in
// the context (i.e. the default no-op propagator injects nothing).
func TestInjectTraceHeaders_NoSpan(t *testing.T) {
	// Ensure we are using the no-op propagator (default state).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	headers := injectTraceHeaders(context.Background())

	// Should not panic and should return zero headers (no active span).
	for _, h := range headers {
		if h.Key == "traceparent" {
			t.Errorf("did not expect traceparent header with no active span, got: %s", h.Value)
		}
	}
}
