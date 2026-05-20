module github.com/realtime-tracking/ingest

go 1.22

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/go-chi/chi/v5 v5.0.12
	github.com/go-playground/validator/v10 v10.22.0
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.19.1
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.52.0
	go.opentelemetry.io/otel v1.27.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.27.0
	go.opentelemetry.io/otel/sdk v1.27.0
	go.opentelemetry.io/otel/trace v1.27.0
	google.golang.org/grpc v1.64.0
)

require (
	pgregory.net/rapid v1.1.0 // test-only
)
