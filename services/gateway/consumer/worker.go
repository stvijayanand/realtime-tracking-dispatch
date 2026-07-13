// Package consumer implements the Kafka consumer worker for the Gateway Service.
package consumer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/realtime-tracking/gateway/config"
	"github.com/realtime-tracking/gateway/session"
)

// filteredEventTypes is the set of event types the Gateway forwards to riders.
// The Gateway does NOT fan out — Kafka does. It translates one event to one
// WebSocket push for the specific connected rider (Requirement 9.4).
var filteredEventTypes = map[string]bool{
	"TripAssigned":  true,
	"TripCancelled": true,
	"TripCompleted": true,
}

// RideEventEnvelope is the Avro-deserialized record for ride-events topic.
// Fields match the Avro schemas in shared/avro/ (TripAssigned, TripCancelled, etc).
type RideEventEnvelope struct {
	EventID    string                 `avro:"event_id"`
	EventType  string                 `avro:"event_type"`
	OccurredAt string                 `avro:"occurred_at"`
	Payload    map[string]interface{} `avro:"payload"`
}

// Worker is a Kafka consumer that polls ride-events, extracts the rider_id,
// and pushes the event payload as a JSON frame over the rider's WebSocket.
//
// On Avro deserialisation failure: logs warning, commits offset, continues.
// W3C traceparent header is extracted from Kafka headers to create a child OTel span.
type Worker struct {
	consumer *confluent.Consumer
	srClient *srclient.SchemaRegistryClient
	registry *session.Registry
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewWorker creates a new Kafka consumer worker for the gateway-consumer-group
// with Avro deserialization via Schema Registry.
func NewWorker(cfg config.Config, registry *session.Registry) (*Worker, error) {
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
		registry: registry,
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

func (w *Worker) run() {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		msg, err := w.consumer.ReadMessage(100)
		if err != nil {
			if kafkaErr, ok := err.(confluent.Error); ok && kafkaErr.Code() == confluent.ErrTimedOut {
				continue
			}
			slog.Warn("kafka read error", "error", err)
			continue
		}

		ctx := extractTraceContext(context.Background(), msg.Headers)
		w.processMessage(ctx, msg)

		if _, err := w.consumer.CommitMessage(msg); err != nil {
			slog.Warn("failed to commit offset", "error", err)
		}
	}
}

func (w *Worker) processMessage(ctx context.Context, msg *confluent.Message) {
	// Deserialise Avro envelope via Schema Registry wire format.
	envelope, err := w.deserializeAvro(msg.Value)
	if err != nil {
		slog.Warn("failed to deserialise avro kafka message",
			"offset", msg.TopicPartition.Offset,
			"error", err)
		return
	}

	eventType := envelope.EventType
	if !filteredEventTypes[eventType] {
		return // not a relevant event type — acknowledge and skip
	}

	// Extract rider_id from payload.
	payload := envelope.Payload
	if payload == nil {
		slog.Warn("event missing payload", "event_type", eventType)
		return
	}
	riderID, _ := payload["rider_id"].(string)
	if riderID == "" {
		slog.Warn("event payload missing rider_id", "event_type", eventType)
		return
	}

	// Build a JSON frame for WebSocket push (riders expect JSON, not Avro binary).
	wsPayload := map[string]interface{}{
		"event_id":    envelope.EventID,
		"event_type":  envelope.EventType,
		"occurred_at": envelope.OccurredAt,
		"payload":     envelope.Payload,
	}
	frame, err := json.Marshal(wsPayload)
	if err != nil {
		slog.Warn("failed to serialise event for websocket", "error", err)
		return
	}

	// Push to the connected rider — no-op if rider is not connected.
	if err := w.registry.Send(riderID, frame); err != nil {
		slog.Debug("rider not connected, skipping push",
			"rider_id", riderID,
			"event_type", eventType)
	}
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
