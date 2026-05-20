package com.dispatch.events;

import java.util.Map;

/**
 * Standard Kafka message envelope for all Domain Events published by the Dispatch Service.
 *
 * <p>Maps to the Avro schemas in {@code shared/avro/}. All Domain Events share this
 * envelope structure — the {@code eventType} field distinguishes them on the same topic.
 *
 * @param eventId    UUID v4 string, unique per event instance. Used as the deduplication
 *                   key for consumer-side idempotency (Phase 2 DynamoDB dedup table).
 * @param eventType  Past-tense domain event name (e.g. "TripAssigned"). Consumers filter
 *                   on this field to route events to the correct handler.
 * @param occurredAt ISO 8601 UTC timestamp of when the event occurred.
 * @param payload    Domain-specific event data. Keys and value types match the Avro schema
 *                   field names in {@code shared/avro/}.
 */
public record DomainEventEnvelope(
    String eventId,
    String eventType,
    String occurredAt,
    Map<String, Object> payload
) {}
