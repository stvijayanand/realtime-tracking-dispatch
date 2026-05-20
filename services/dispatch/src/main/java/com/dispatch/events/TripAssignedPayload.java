package com.dispatch.events;

/**
 * Payload for the {@code TripAssigned} Domain Event.
 *
 * <p>Matches the Avro schema at {@code shared/avro/trip_assigned.avsc}.
 * Published to the {@code ride-events} topic when the Dispatch Service assigns
 * a driver to a trip. Consumed by the Notification Service (push log) and
 * Gateway Service (WebSocket push to rider).
 *
 * @param tripId     UUID v4 identifying the Trip aggregate
 * @param driverId   identifier of the assigned driver
 * @param riderId    identifier of the rider who requested the trip
 * @param assignedAt ISO 8601 UTC timestamp when the driver was assigned
 */
public record TripAssignedPayload(
    String tripId,
    String driverId,
    String riderId,
    String assignedAt
) {}
