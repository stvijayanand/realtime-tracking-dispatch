// Package consumer implements the Kafka consumer worker for the Notification Service.
package consumer

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/realtime-tracking/notification/config"
	"github.com/realtime-tracking/notification/handler"
	"github.com/realtime-tracking/notification/logger"
)

// RideEventEnvelope is the Avro-deserialized record for ride-events topic.
// Fields match the Avro schemas in shared/avro/ (TripAssigned, TripRequested, etc).
// The Payload field uses map[string]interface{} for flexibility across event types.
type RideEventEnvelope struct {
	EventID    string                 `avro:"event_id"`
	EventType  string                 `avro:"event_type"`
	OccurredAt string                 `avro:"occurred_at"`
	Payload    map[string]interface{} `avro:"payload"`
}

// Worker is a Kafka consumer that polls messages, routes them to handlers by
// event_type, and commits offsets after each message is processed.
//
// On Avro deserialisation failure: calls log.LogWarning() with raw bytes,
// commits offset, and continues — never crashes (Requirement 4.4).
//
// W3C traceparent header is extracted from Kafka message headers to create a
// child OTel span, maintaining the distributed trace (Requirement 12.1).
type Worker struct {
	consumer *confluent.Consumer
	srClient *srclient.SchemaRegistryClient
	handlers map[string]handler.HandlerFunc
	log      *logger.Logger
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewWorker creates a new Kafka consumer worker configured with SASL/PLAIN,
// Avro deserialization via Schema Registry, and the provided handler map.
func NewWorker(cfg config.Config, handlers map[string]handler.HandlerFunc, log *logger.Logger) (*Worker, error) {
	c, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrapServers,
		"group.id":           cfg.KafkaConsumerGroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanisms":    "PLAIN",
		"sasl.username":      cfg.KafkaSASLUsername,
		"sasl.password":      cfg.KafkaSASLPassword,
	})
	if err != nil {
		return nil, err
	}

	if err := c.Subscribe(cfg.KafkaTopic, nil); err != nil {
		c.Close()
		return nil, err
	}

	// Create Schema Registry client for schema lookup during deserialization.
	srClient := srclient.CreateSchemaRegistryClient(cfg.SchemaRegistryURL)

	return &Worker{
		consumer: c,
		srClient: srClient,
		handlers: handlers,
		log:      log,
		stopCh:   make(chan struct{}),
	}, nil
}

// Start launches the consumer run loop in a background goroutine.
func (w *Worker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run()
	}()
}

// Stop signals the run loop to exit and waits for it to finish.
func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	w.consumer.Close()
}

// run is the main consumer loop. It polls for messages, deserialises them,
// routes to the appropriate handler, and commits offsets.
func (w *Worker) run() {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		msg, err := w.consumer.ReadMessage(100) // 100ms poll timeout
		if err != nil {
			// Timeout is normal — just continue polling.
			if kafkaErr, ok := err.(confluent.Error); ok && kafkaErr.Code() == confluent.ErrTimedOut {
				continue
			}
			// Other errors: log and continue.
			w.log.LogWarning(context.Background(), "kafka read error", []byte(err.Error()))
			continue
		}

		ctx := extractTraceContext(context.Background(), msg.Headers)
		w.processMessage(ctx, msg)

		// Commit offset after processing (manual commit for at-least-once delivery).
		if _, err := w.consumer.CommitMessage(msg); err != nil {
			w.log.LogWarning(ctx, "failed to commit offset", []byte(err.Error()))
		}
	}
}

// processMessage deserialises the Kafka message using Avro and routes it to the correct handler.
func (w *Worker) processMessage(ctx context.Context, msg *confluent.Message) {
	// Deserialise Avro envelope via Schema Registry wire format.
	envelope, err := w.deserializeAvro(msg.Value)
	if err != nil {
		w.log.LogWarning(ctx, fmt.Sprintf("failed to deserialise avro kafka message: %v", err), msg.Value)
		return
	}

	// Convert to map for handler compatibility.
	envelopeMap := map[string]interface{}{
		"event_id":    envelope.EventID,
		"event_type":  envelope.EventType,
		"occurred_at": envelope.OccurredAt,
		"payload":     envelope.Payload,
	}

	// Extract event_type for routing.
	eventType := envelope.EventType

	// Route to handler — fall back to NoOpHandler for unknown event types.
	h, ok := w.handlers[eventType]
	if !ok {
		h = handler.NoOpHandler
	}
	h(ctx, envelopeMap)
}

// deserializeAvro decodes a Confluent Schema Registry wire-format message:
// [magic byte (0)] [4-byte schema ID (big-endian)] [avro binary payload]
func (w *Worker) deserializeAvro(data []byte) (*RideEventEnvelope, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short for avro wire format: %d bytes", len(data))
	}
	if data[0] != 0 {
		return nil, fmt.Errorf("invalid magic byte: expected 0, got %d", data[0])
	}

	schemaID := binary.BigEndian.Uint32(data[1:5])
	avroPayload := data[5:]

	// Fetch schema from registry by ID (srclient caches internally).
	schema, err := w.srClient.GetSchema(int(schemaID))
	if err != nil {
		return nil, fmt.Errorf("fetching schema ID %d from registry: %w", schemaID, err)
	}

	// Parse the Avro schema and unmarshal the binary payload.
	parsedSchema, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("parsing avro schema: %w", err)
	}

	var envelope RideEventEnvelope
	if err := avro.Unmarshal(parsedSchema, avroPayload, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshalling avro payload: %w", err)
	}

	return &envelope, nil
}

// extractTraceContext extracts the W3C traceparent header from Kafka message
// headers and returns a context with the parent span set.
func extractTraceContext(ctx context.Context, headers []confluent.Header) context.Context {
	carrier := propagation.MapCarrier{}
	for _, h := range headers {
		carrier[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
