package com.dispatch.events;

/**
 * Payload for the {@code TripCancelled} Domain Event.
 *
 * <p>Matches the Avro schema at {@code shared/avro/trip_cancelled.avsc}.
 * Modelled in Phase 1 as a first-class compensating event in the domain model;
 * not triggered by any service in Phase 1. The Saga State Monitor (Phase 2)
 * will emit this event for trips stuck in intermediate states beyond a timeout
 * threshold (see ADR 006).
 *
 * @param tripId      UUID v4 identifying the Trip aggregate being cancelled
 * @param reason      human-readable description of why the trip was cancelled
 * @param cancelledAt ISO 8601 UTC timestamp when the trip was cancelled
 */
public record TripCancelledPayload(
    String tripId,
    String reason,
    String cancelledAt
) {}
