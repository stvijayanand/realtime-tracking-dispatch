package com.dispatch.shared;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Standard Kafka message envelope for all Domain Events in the dispatch platform.
 *
 * <p>This record is an infrastructure-level type used for Avro serialisation/deserialisation
 * of Domain Events over Kafka. It is intentionally kept in {@code shared/} because it
 * represents the Kafka integration contract, not a domain concept.
 *
 * <p><strong>Usage note:</strong> The {@link #of(String, Map)} factory is for
 * infrastructure-level use only (e.g., test utilities, generic consumers). Service-specific
 * domain events must use {@code EventEnvelopeFactory} in the Dispatch Service, which
 * isolates UUID and timestamp generation for testability.
 *
 * @param eventId    UUID v4 string, unique per Domain Event instance. Used as the
 *                   deduplication key for consumer-side idempotency (Phase 2).
 * @param eventType  Past-tense domain event name (e.g., "TripAssigned"). Consumers
 *                   filter on this field to route events to the correct handler.
 * @param occurredAt ISO 8601 UTC timestamp of when the event occurred.
 * @param payload    Domain-specific event data. Keys and value types are defined
 *                   by the Avro schema in {@code shared/avro/}.
 */
public record KafkaEnvelope(
        String eventId,
        String eventType,
        String occurredAt,
        Map<String, Object> payload) {

    /**
     * Creates a new {@code KafkaEnvelope} with a generated {@code eventId} (UUID v4)
     * and {@code occurredAt} (current UTC time, ISO 8601).
     *
     * <p><strong>Infrastructure use only.</strong> Service-specific domain events must
     * use {@code EventEnvelopeFactory.buildTripRequested()} or
     * {@code EventEnvelopeFactory.buildTripAssigned()} in the Dispatch Service.
     * Those factories isolate UUID and timestamp generation for deterministic testing.
     *
     * @param eventType the past-tense domain event name (e.g., "TripAssigned")
     * @param payload   the domain-specific event data map
     * @return a new {@code KafkaEnvelope} with generated {@code eventId} and {@code occurredAt}
     */
    public static KafkaEnvelope of(String eventType, Map<String, Object> payload) {
        return new KafkaEnvelope(
                UUID.randomUUID().toString(),
                eventType,
                Instant.now().toString(),
                payload);
    }
}
