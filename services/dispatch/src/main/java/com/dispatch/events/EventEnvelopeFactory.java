package com.dispatch.events;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Static factory for building Domain Event envelopes.
 *
 * <p>All UUID generation ({@code eventId}) and timestamp generation ({@code occurredAt})
 * are isolated here. {@code DispatchService} NEVER calls {@code UUID.randomUUID()}
 * directly — it always delegates to this factory. This isolation makes the service
 * layer deterministic and straightforward to test with jqwik property tests.
 *
 * <p>This factory is for service-layer use only. Infrastructure-level generic envelope
 * creation uses {@code KafkaEnvelope.of()} in {@code shared/KafkaEnvelope.java}.
 */
public final class EventEnvelopeFactory {

    private EventEnvelopeFactory() {
        // Utility class — not instantiable.
    }

    /**
     * Builds a {@code TripRequested} Domain Event envelope.
     *
     * <p>Generates a new {@code eventId} (UUID v4) and {@code occurredAt}
     * (current UTC time, ISO 8601) at call time.
     *
     * @param tripId      UUID of the Trip aggregate
     * @param riderId     identifier of the requesting rider
     * @param pickupLat   WGS-84 latitude of the pickup location
     * @param pickupLng   WGS-84 longitude of the pickup location
     * @param requestedAt timestamp when the ride was requested
     * @return a fully-populated {@link DomainEventEnvelope} ready for Kafka publish
     */
    public static DomainEventEnvelope buildTripRequested(
            UUID tripId,
            String riderId,
            double pickupLat,
            double pickupLng,
            Instant requestedAt) {

        Map<String, Object> payload = Map.of(
            "trip_id",         tripId.toString(),
            "rider_id",        riderId,
            "pickup_location", Map.of("latitude", pickupLat, "longitude", pickupLng),
            "requested_at",    requestedAt.toString()
        );

        return new DomainEventEnvelope(
            UUID.randomUUID().toString(),
            "TripRequested",
            Instant.now().toString(),
            payload
        );
    }

    /**
     * Builds a {@code TripAssigned} Domain Event envelope.
     *
     * <p>Generates a new {@code eventId} (UUID v4) and {@code occurredAt}
     * (current UTC time, ISO 8601) at call time.
     *
     * @param tripId     UUID of the Trip aggregate
     * @param driverId   identifier of the assigned driver
     * @param riderId    identifier of the rider
     * @param assignedAt timestamp when the driver was assigned
     * @return a fully-populated {@link DomainEventEnvelope} ready for Kafka publish
     */
    public static DomainEventEnvelope buildTripAssigned(
            UUID tripId,
            String driverId,
            String riderId,
            Instant assignedAt) {

        Map<String, Object> payload = Map.of(
            "trip_id",     tripId.toString(),
            "driver_id",   driverId,
            "rider_id",    riderId,
            "assigned_at", assignedAt.toString()
        );

        return new DomainEventEnvelope(
            UUID.randomUUID().toString(),
            "TripAssigned",
            Instant.now().toString(),
            payload
        );
    }
}
