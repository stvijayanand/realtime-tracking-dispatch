package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/realtime-tracking/ingest/config"
	"github.com/realtime-tracking/ingest/events"
)

// Producer wraps a confluent Kafka producer with Avro serialisation via
// Schema Registry. On first publish the schema is auto-registered with the
// registry, enforcing the contract in shared/avro/location_ping_received.avsc.
type Producer struct {
	producer *confluent.Producer
	srClient *srclient.SchemaRegistryClient
	topic    string
	schema   *srclient.Schema // cached after first registration
}

// LocationPingReceivedPayload matches the nested payload record in the Avro schema.
type LocationPingReceivedPayload struct {
	DriverID  string  `avro:"driver_id"`
	Latitude  float64 `avro:"latitude"`
	Longitude float64 `avro:"longitude"`
	Timestamp string  `avro:"timestamp"`
}

// LocationPingReceivedEvent is the top-level Avro record for the
// LocationPingReceived domain event. Field names and types match
// shared/avro/location_ping_received.avsc exactly.
type LocationPingReceivedEvent struct {
	EventID    string                      `avro:"event_id"`
	EventType  string                      `avro:"event_type"`
	OccurredAt string                      `avro:"occurred_at"`
	Payload    LocationPingReceivedPayload `avro:"payload"`
}

// avroSchema is the Avro schema string for LocationPingReceived, matching
// shared/avro/location_ping_received.avsc.
const avroSchema = `{
  "type": "record",
  "namespace": "com.dispatch.events",
  "name": "LocationPingReceived",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_type", "type": "string", "default": "LocationPingReceived"},
    {"name": "occurred_at", "type": "string"},
    {"name": "payload", "type": {
      "type": "record",
      "name": "LocationPingReceivedPayload",
      "fields": [
        {"name": "driver_id", "type": "string"},
        {"name": "latitude", "type": "double"},
        {"name": "longitude", "type": "double"},
        {"name": "timestamp", "type": "string"}
      ]
    }}
  ]
}`

// parsedSchema is the hamba/avro parsed schema for binary serialisation.
var parsedSchema = avro.MustParse(avroSchema)

// NewProducer creates a new Kafka producer configured with:
//   - acks=all for strongest delivery guarantee
//   - enable.idempotence=true to prevent duplicate messages on retry
//   - SASL/PLAIN authentication from config
//   - Avro serialisation via Schema Registry (auto-register on first publish)
//
// Returns an error if the underlying confluent producer or Schema Registry
// client cannot be created.
func NewProducer(cfg config.Config) (*Producer, error) {
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrapServers,
		"acks":               "all",
		"enable.idempotence": true,
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanisms":    "PLAIN",
		"sasl.username":      cfg.KafkaSASLUsername,
		"sasl.password":      cfg.KafkaSASLPassword,
	})
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer: %w", err)
	}

	// Create Schema Registry client for schema registration.
	srClient := srclient.CreateSchemaRegistryClient(cfg.SchemaRegistryURL)

	return &Producer{
		producer: p,
		srClient: srClient,
		topic:    cfg.KafkaTopic,
	}, nil
}

// getOrRegisterSchema lazily registers the Avro schema with Schema Registry
// on first call (RecordNameStrategy: subject = fully-qualified record name,
// e.g. "com.dispatch.events.LocationPingReceived"). Subsequent calls return
// the cached schema. This is thread-safe via srclient's internal caching.
func (p *Producer) getOrRegisterSchema() (*srclient.Schema, error) {
	if p.schema != nil {
		return p.schema, nil
	}

	// RecordNameStrategy: subject = namespace.name from the Avro schema.
	subject := "com.dispatch.events.LocationPingReceived"
	schema, err := p.srClient.CreateSchema(subject, avroSchema, srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("registering schema with registry: %w", err)
	}
	p.schema = schema
	return schema, nil
}

// Publish serialises the event as Avro via Schema Registry, produces it to
// Kafka with driver_id as the message key, injects the W3C traceparent header
// from the current OTel span context, waits for delivery confirmation, and
// returns the event_id on success.
//
// Returns an error on delivery failure — the caller should return HTTP 503.
func (p *Producer) Publish(ctx context.Context, key string, event events.DomainEvent) (string, error) {
	// Ensure schema is registered with Schema Registry.
	schema, err := p.getOrRegisterSchema()
	if err != nil {
		return "", err
	}

	// Build the typed Avro record from the domain event.
	avroEvent := LocationPingReceivedEvent{
		EventID:    event.EventID,
		EventType:  event.EventType,
		OccurredAt: event.OccurredAt,
		Payload: LocationPingReceivedPayload{
			DriverID:  event.Payload["driver_id"].(string),
			Latitude:  event.Payload["latitude"].(float64),
			Longitude: event.Payload["longitude"].(float64),
			Timestamp: event.Payload["timestamp"].(string),
		},
	}

	// Serialize to Avro binary.
	avroBytes, err := avro.Marshal(parsedSchema, avroEvent)
	if err != nil {
		return "", fmt.Errorf("serialising event as avro: %w", err)
	}

	// Build the Confluent Schema Registry wire format:
	// [magic byte (0)] [4-byte schema ID (big-endian)] [avro binary payload]
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
	valueBytes := make([]byte, 0, 1+4+len(avroBytes))
	valueBytes = append(valueBytes, 0) // magic byte
	valueBytes = append(valueBytes, schemaIDBytes...)
	valueBytes = append(valueBytes, avroBytes...)

	// Inject W3C traceparent/tracestate headers from the current OTel span context.
	headers := injectTraceHeaders(ctx)

	// Buffered channel (size 1) so Produce() returns immediately without blocking.
	deliveryChan := make(chan confluent.Event, 1)
	err = p.producer.Produce(&confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &p.topic,
			Partition: confluent.PartitionAny,
		},
		Key:     []byte(key),
		Value:   valueBytes,
		Headers: headers,
	}, deliveryChan)
	if err != nil {
		return "", fmt.Errorf("producing kafka message: %w", err)
	}

	// Block until the broker acknowledges (or rejects) the message.
	e := <-deliveryChan
	msg, ok := e.(*confluent.Message)
	if !ok {
		return "", fmt.Errorf("unexpected kafka event type: %T", e)
	}
	if msg.TopicPartition.Error != nil {
		return "", fmt.Errorf("kafka delivery failed: %w", msg.TopicPartition.Error)
	}

	return event.EventID, nil
}

// Close flushes any pending messages (up to 5 seconds) and closes the
// underlying confluent producer. Should be called on graceful shutdown.
func (p *Producer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}

// injectTraceHeaders extracts the W3C traceparent and tracestate values from
// the current OTel span context and returns them as Kafka message headers.
// If no active span is present the propagator injects nothing and an empty
// slice is returned — this is safe and expected in tests without a tracer.
func injectTraceHeaders(ctx context.Context) []confluent.Header {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	headers := make([]confluent.Header, 0, len(carrier))
	for k, v := range carrier {
		headers = append(headers, confluent.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return headers
}
