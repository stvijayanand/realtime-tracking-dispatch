package config

import (
	"log"
	"os"
)

// Config holds all required environment-variable-driven configuration for the
// Ingest Service. Every field is mandatory — LoadConfig exits non-zero if any
// variable is absent (Requirement 2.8, 10.10).
type Config struct {
	// KafkaBootstrapServers is the comma-separated list of Kafka broker addresses.
	// Env var: KAFKA_BOOTSTRAP_SERVERS
	KafkaBootstrapServers string

	// KafkaTopic is the name of the Kafka topic for GPS ping events.
	// Env var: KAFKA_TOPIC_GPS_PINGS
	KafkaTopic string

	// KafkaSASLUsername is the SASL/PLAIN username for Kafka authentication.
	// Env var: KAFKA_SASL_USERNAME
	KafkaSASLUsername string

	// KafkaSASLPassword is the SASL/PLAIN password for Kafka authentication.
	// Env var: KAFKA_SASL_PASSWORD
	KafkaSASLPassword string

	// SchemaRegistryURL is the base URL of the Confluent Schema Registry.
	// Env var: SCHEMA_REGISTRY_URL
	SchemaRegistryURL string

	// ServicePort is the TCP port the HTTP server listens on (e.g. "8001").
	// Env var: SERVICE_PORT
	ServicePort string

	// OTELEndpoint is the OTLP gRPC endpoint for OpenTelemetry trace export.
	// Env var: OTEL_EXPORTER_OTLP_ENDPOINT
	OTELEndpoint string
}

// LoadConfig reads all required environment variables and returns a populated
// Config. If any required variable is absent or empty, LoadConfig logs a
// descriptive error identifying the missing variable and calls os.Exit(1) so
// the service never starts with incomplete configuration.
func LoadConfig() Config {
	return Config{
		KafkaBootstrapServers: mustGetenv("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaTopic:            mustGetenv("KAFKA_TOPIC_GPS_PINGS"),
		KafkaSASLUsername:     mustGetenv("KAFKA_SASL_USERNAME"),
		KafkaSASLPassword:     mustGetenv("KAFKA_SASL_PASSWORD"),
		SchemaRegistryURL:     mustGetenv("SCHEMA_REGISTRY_URL"),
		ServicePort:           mustGetenv("SERVICE_PORT"),
		OTELEndpoint:          mustGetenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
}

// mustGetenv returns the value of the named environment variable. If the
// variable is unset or empty, it logs a descriptive fatal error and exits with
// status 1 (Requirement 10.10).
func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
}
