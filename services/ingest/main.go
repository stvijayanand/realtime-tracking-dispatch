package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/realtime-tracking/ingest/config"
	"github.com/realtime-tracking/ingest/handler"
	"github.com/realtime-tracking/ingest/kafka"
	ingestMiddleware "github.com/realtime-tracking/ingest/middleware"
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

	// Construct Kafka producer.
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	// Write OpenAPI spec at startup (Requirement 2.7, 8.1).
	if err := writeOpenAPISpec(); err != nil {
		log.Printf("warning: failed to write openapi.json: %v", err)
	}

	// Build chi router.
	r := chi.NewRouter()
	r.Use(chiMiddleware.RealIP)
	r.Use(chiMiddleware.Recoverer)

	// Health and metrics endpoints — no body size limit, no OTel tracing overhead.
	r.Get("/health", handler.HealthHandler)
	r.Handle("/metrics", promhttp.Handler())

	// POST /location: MaxBodySize → OTel instrumentation → handler.
	locationHandler := &handler.LocationHandler{
		Producer:  producer,
		Validator: validator.New(),
	}
	r.With(ingestMiddleware.MaxBodySize(65536)).
		Method(http.MethodPost, "/location",
			otelhttp.NewHandler(locationHandler, "POST /location"))

	// Start HTTP server.
	srv := &http.Server{
		Addr:         ":" + cfg.ServicePort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("ingest service listening on :%s", cfg.ServicePort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-quit
	log.Println("shutting down ingest service...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	log.Println("ingest service stopped")
}

// initTracer creates an OTLP gRPC exporter and returns a configured TracerProvider.
// Sets the global tracer provider and W3C text map propagator.
//
// The gRPC connection is established with a 5-second dial timeout so the service
// fails fast if the collector is unreachable at startup.
func initTracer(otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext( //nolint:staticcheck // DialContext is the idiomatic approach for connection-reuse with OTLP exporter
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

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "ingest-service"),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// writeOpenAPISpec writes the static OpenAPI 3.0.3 spec to openapi.json in the
// same directory as the binary (Requirement 2.7, 8.1). In Phase 1 this is a
// static file; Phase 2 can replace this with auto-generation from route metadata.
func writeOpenAPISpec() error {
	const specPath = "openapi.json"
	// Only write if the file does not already exist — avoids overwriting a
	// manually curated spec on repeated restarts.
	if _, err := os.Stat(specPath); err == nil {
		return nil
	}

	spec := openAPISpec()
	return os.WriteFile(specPath, []byte(spec), 0o644)
}

// openAPISpec returns the static OpenAPI 3.0.3 JSON document describing the
// Ingest Service API surface.
func openAPISpec() string {
	return `{
  "openapi": "3.0.3",
  "info": {
    "title": "Ingest Service",
    "version": "1.0.0",
    "description": "GPS ping ingestion service. Receives driver location updates and publishes LocationPingReceived Domain Events to Kafka."
  },
  "paths": {
    "/location": {
      "post": {
        "summary": "Submit a GPS ping",
        "operationId": "submitGpsPing",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/GpsPingRequest"
              }
            }
          }
        },
        "responses": {
          "202": {
            "description": "GPS ping accepted and published to Kafka",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AcceptedResponse"
                }
              }
            }
          },
          "413": {
            "description": "Request body exceeds 64 KB"
          },
          "422": {
            "description": "Validation error — missing or invalid fields",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ValidationErrorResponse"
                }
              }
            }
          },
          "503": {
            "description": "Kafka topic unavailable — GPS ping could not be published"
          }
        }
      }
    },
    "/health": {
      "get": {
        "summary": "Health check",
        "operationId": "healthCheck",
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HealthResponse"
                }
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
        "responses": {
          "200": {
            "description": "Prometheus text format metrics",
            "content": {
              "text/plain": {
                "schema": {
                  "type": "string"
                }
              }
            }
          }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "GpsPingRequest": {
        "type": "object",
        "required": ["driver_id", "latitude", "longitude", "timestamp"],
        "properties": {
          "driver_id": {
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "description": "Unique identifier for the driver emitting the GPS ping"
          },
          "latitude": {
            "type": "number",
            "format": "double",
            "minimum": -90.0,
            "maximum": 90.0,
            "description": "WGS-84 latitude in decimal degrees"
          },
          "longitude": {
            "type": "number",
            "format": "double",
            "minimum": -180.0,
            "maximum": 180.0,
            "description": "WGS-84 longitude in decimal degrees"
          },
          "timestamp": {
            "type": "string",
            "format": "date-time",
            "description": "ISO 8601 timestamp of the GPS ping (device clock)"
          }
        }
      },
      "AcceptedResponse": {
        "type": "object",
        "required": ["message_id"],
        "properties": {
          "message_id": {
            "type": "string",
            "format": "uuid",
            "description": "UUID of the published LocationPingReceived Domain Event (equals event_id in the Kafka message)"
          }
        }
      },
      "HealthResponse": {
        "type": "object",
        "required": ["status"],
        "properties": {
          "status": {
            "type": "string",
            "example": "ok"
          }
        }
      },
      "ValidationErrorDetail": {
        "type": "object",
        "required": ["field", "message"],
        "properties": {
          "field": {
            "type": "string",
            "description": "Name of the field that failed validation"
          },
          "message": {
            "type": "string",
            "description": "Validation rule that was violated"
          }
        }
      },
      "ValidationErrorResponse": {
        "type": "object",
        "required": ["detail"],
        "properties": {
          "detail": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/ValidationErrorDetail"
            }
          }
        }
      }
    }
  }
}`
}
