package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/realtime-tracking/notification/config"
	"github.com/realtime-tracking/notification/consumer"
	"github.com/realtime-tracking/notification/events"
	"github.com/realtime-tracking/notification/handler"
	notifLogger "github.com/realtime-tracking/notification/logger"
)

func main() {
	cfg := config.LoadConfig()

	// Initialise OpenTelemetry tracer.
	tp, err := initTracer(cfg.OTELEndpoint)
	if err != nil {
		log.Fatalf("failed to initialise OTel tracer: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("error shutting down tracer provider: %v", err)
		}
	}()

	// Construct structured logger.
	l := notifLogger.New()

	// Build handler map: TripAssigned → log notification; all others → no-op.
	handlers := map[string]handler.HandlerFunc{
		"TripAssigned": func(ctx context.Context, envelope map[string]interface{}) {
			event, err := events.ParseTripAssigned(envelope)
			if err != nil {
				l.LogWarning(ctx, "failed to parse TripAssigned event", []byte(err.Error()))
				return
			}
			handler.HandleTripAssigned(ctx, event, l)
		},
	}

	// Construct Kafka consumer worker.
	w, err := consumer.NewWorker(cfg, handlers, l)
	if err != nil {
		log.Fatalf("failed to create kafka consumer worker: %v", err)
	}
	w.Start()

	// Write OpenAPI spec at startup (Requirement 4.5, 8.2).
	if err := writeOpenAPISpec(); err != nil {
		log.Printf("warning: failed to write openapi.json: %v", err)
	}

	// Build HTTP server with health and metrics endpoints.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":" + cfg.ServicePort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("notification service listening on :%s", cfg.ServicePort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-quit
	log.Println("shutting down notification service...")

	w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	log.Println("notification service stopped")
}

// initTracer creates an OTLP gRPC exporter and returns a configured TracerProvider.
func initTracer(otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext( //nolint:staticcheck
		ctx,
		otlpEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to OTLP endpoint %s: %w", otlpEndpoint, err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// writeOpenAPISpec writes the static OpenAPI 3.0.3 spec to openapi.json.
func writeOpenAPISpec() error {
	const specPath = "openapi.json"
	if _, err := os.Stat(specPath); err == nil {
		return nil // already exists
	}
	spec := `{
  "openapi": "3.0.3",
  "info": {
    "title": "Notification Service",
    "version": "1.0.0",
    "description": "Consumes TripAssigned Domain Events from Kafka and writes structured JSON notification log lines to stdout."
  },
  "paths": {
    "/health": {
      "get": {
        "summary": "Health check",
        "operationId": "healthCheck",
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "schema": { "type": "object", "properties": { "status": { "type": "string", "example": "ok" } } }
              }
            }
          }
        }
      }
    },
    "/metrics": {
      "get": {
        "summary": "Prometheus metrics",
        "operationId": "getMetrics",
        "responses": { "200": { "description": "Prometheus text format metrics" } }
      }
    }
  }
}`
	return os.WriteFile(specPath, []byte(spec), 0o644)
}
