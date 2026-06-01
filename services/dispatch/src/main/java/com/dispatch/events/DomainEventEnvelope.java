package com.dispatch.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Standard Kafka message envelope for all Domain Events published by the Dispatch Service.
 *
 * <p>Fields are serialised with snake_case names to match the Go consumer contracts
 * (notification-service, gateway-service) which expect: event_id, event_type,
 * occurred_at, payload.
 *
 * @param eventId    UUID v4 string, unique per event instance.
 * @param eventType  Past-tense domain event name (e.g. "TripAssigned").
 * @param occurredAt ISO 8601 UTC timestamp of when the event occurred.
 * @param payload    Domain-specific event data.
 */
public record DomainEventEnvelope(
    @JsonProperty("event_id")    String eventId,
    @JsonProperty("event_type")  String eventType,
    @JsonProperty("occurred_at") String occurredAt,
    @JsonProperty("payload")     Map<String, Object> payload
) {}
