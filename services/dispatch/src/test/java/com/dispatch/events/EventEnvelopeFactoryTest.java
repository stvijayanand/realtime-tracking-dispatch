package com.dispatch.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventEnvelopeFactoryTest {

    // ── buildTripRequested ────────────────────────────────────────────────────

    @Test
    void buildTripRequested_generatesNonEmptyEventId() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripRequested(
            UUID.randomUUID(), "rider-1", 51.5, -0.1, Instant.now());

        assertNotNull(env.eventId());
        assertFalse(env.eventId().isBlank());
        // Must be a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(env.eventId()));
    }

    @Test
    void buildTripRequested_setsCorrectEventType() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripRequested(
            UUID.randomUUID(), "rider-1", 51.5, -0.1, Instant.now());

        assertEquals("TripRequested", env.eventType());
    }

    @Test
    void buildTripRequested_occurredAtIsValidIso8601() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripRequested(
            UUID.randomUUID(), "rider-1", 51.5, -0.1, Instant.now());

        assertNotNull(env.occurredAt());
        assertDoesNotThrow(() -> Instant.parse(env.occurredAt()),
            "occurredAt must be a valid ISO 8601 timestamp");
    }

    @Test
    void buildTripRequested_payloadContainsAllFields() {
        UUID tripId = UUID.randomUUID();
        String riderId = "rider-abc";
        double lat = 51.5074;
        double lng = -0.1278;
        Instant requestedAt = Instant.now();

        DomainEventEnvelope env = EventEnvelopeFactory.buildTripRequested(
            tripId, riderId, lat, lng, requestedAt);

        Map<String, Object> payload = env.payload();
        assertEquals(tripId.toString(), payload.get("trip_id"));
        assertEquals(riderId, payload.get("rider_id"));
        assertEquals(requestedAt.toString(), payload.get("requested_at"));

        @SuppressWarnings("unchecked")
        Map<String, Object> pickup = (Map<String, Object>) payload.get("pickup_location");
        assertNotNull(pickup, "pickup_location must be present");
        assertEquals(lat, pickup.get("latitude"));
        assertEquals(lng, pickup.get("longitude"));
    }

    // ── buildTripAssigned ─────────────────────────────────────────────────────

    @Test
    void buildTripAssigned_generatesNonEmptyEventId() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripAssigned(
            UUID.randomUUID(), "driver-001", "rider-1", Instant.now());

        assertNotNull(env.eventId());
        assertFalse(env.eventId().isBlank());
        assertDoesNotThrow(() -> UUID.fromString(env.eventId()));
    }

    @Test
    void buildTripAssigned_setsCorrectEventType() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripAssigned(
            UUID.randomUUID(), "driver-001", "rider-1", Instant.now());

        assertEquals("TripAssigned", env.eventType());
    }

    @Test
    void buildTripAssigned_occurredAtIsValidIso8601() {
        DomainEventEnvelope env = EventEnvelopeFactory.buildTripAssigned(
            UUID.randomUUID(), "driver-001", "rider-1", Instant.now());

        assertDoesNotThrow(() -> Instant.parse(env.occurredAt()),
            "occurredAt must be a valid ISO 8601 timestamp");
    }

    @Test
    void buildTripAssigned_payloadContainsAllFields() {
        UUID tripId = UUID.randomUUID();
        String driverId = "driver-001";
        String riderId = "rider-abc";
        Instant assignedAt = Instant.now();

        DomainEventEnvelope env = EventEnvelopeFactory.buildTripAssigned(
            tripId, driverId, riderId, assignedAt);

        Map<String, Object> payload = env.payload();
        assertEquals(tripId.toString(), payload.get("trip_id"));
        assertEquals(driverId, payload.get("driver_id"));
        assertEquals(riderId, payload.get("rider_id"));
        assertEquals(assignedAt.toString(), payload.get("assigned_at"));
    }

    // ── uniqueness ────────────────────────────────────────────────────────────

    @Test
    void twoCallsProduceDifferentEventIds() {
        UUID tripId = UUID.randomUUID();
        DomainEventEnvelope e1 = EventEnvelopeFactory.buildTripAssigned(
            tripId, "driver-001", "rider-1", Instant.now());
        DomainEventEnvelope e2 = EventEnvelopeFactory.buildTripAssigned(
            tripId, "driver-001", "rider-1", Instant.now());

        assertNotEquals(e1.eventId(), e2.eventId(),
            "Each call must generate a unique eventId");
    }
}
