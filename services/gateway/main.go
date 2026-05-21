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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/realtime-tracking/gateway/config"
	"github.com/realtime-tracking/gateway/consumer"
	"github.com/realtime-tracking/gateway/handler"
	"github.com/realtime-tracking/gateway/session"
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

	// Construct session registry.
	registry := session.NewRegistry()

	// Register active WebSocket connections gauge (Requirement 12.2).
	activeConns := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gateway_active_websocket_connections",
			Help: "Number of active WebSocket connections.",
		},
		func() float64 { return float64(registry.Count()) },
	)
	prometheus.MustRegister(activeConns)

	// Construct Kafka consumer worker.
	w, err := consumer.NewWorker(cfg, registry)
	if err != nil {
		log.Fatalf("failed to create kafka consumer worker: %v", err)
	}
	w.Start()

	// Build HTTP/WebSocket router.
	mux := http.NewServeMux()

	// WebSocket endpoint — OTel instrumented.
	wsHandler := &handler.WebSocketHandler{Registry: registry}
	mux.Handle("/ws", otelhttp.NewHandler(wsHandler, "GET /ws"))

	// Health and metrics.
	mux.HandleFunc("/health", handler.HealthHandler)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":" + cfg.ServicePort,
		Handler:      mux,
		ReadTimeout:  60 * time.Second, // longer for WebSocket connections
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("gateway service listening on :%s", cfg.ServicePort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-quit
	log.Println("shutting down gateway service...")

	w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	log.Println("gateway service stopped")
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
