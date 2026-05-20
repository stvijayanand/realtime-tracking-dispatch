package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/realtime-tracking/ingest/config"
	"github.com/realtime-tracking/ingest/events"
)

// Producer wraps a confluent Kafka producer with Avro-compatible serialisation.
// Schema Registry integration is wired in when the full Avro serialiser is
// available; Phase 1 uses JSON serialisation of the envelope map, which is
// structurally identical to the Avro schema contract.
type Producer struct {
	producer          *confluent.Producer
	schemaRegistryURL string
	topic             string
}

// NewProducer creates a new Kafka producer configured with:
//   - acks=all for strongest delivery guarantee
//   - enable.idempotence=true to prevent duplicate messages on retry
//   - SASL/PLAIN authentication from config
//
// Returns an error if the underlying confluent producer cannot be created
// (e.g. invalid bootstrap servers format).
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
	return &Producer{
		producer:          p,
		schemaRegistryURL: cfg.SchemaRegistryURL,
		topic:             cfg.KafkaTopic,
	}, nil
}

// Publish serialises the event as a JSON-encoded envelope (Avro-compatible
// field layout), produces it to Kafka with driver_id as the message key,
// injects the W3C traceparent header from the current OTel span context,
// waits for delivery confirmation, and returns the event_id on success.
//
// Returns an error on delivery failure — the caller should return HTTP 503.
// The delivery channel is buffered (size 1) so Produce() never blocks.
func (p *Producer) Publish(ctx context.Context, key string, event events.DomainEvent) (string, error) {
	// Serialise the event envelope as JSON bytes.
	// The field names match the Avro schema in shared/avro/location_ping_received.avsc.
	payload := map[string]interface{}{
		"event_id":    event.EventID,
		"event_type":  event.EventType,
		"occurred_at": event.OccurredAt,
		"payload":     event.Payload,
	}
	valueBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("serialising event: %w", err)
	}

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
