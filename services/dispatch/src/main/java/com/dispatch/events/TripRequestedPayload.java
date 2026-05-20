package com.dispatch.events;

/**
 * Payload for the {@code TripRequested} Domain Event.
 *
 * <p>Matches the Avro schema at {@code shared/avro/trip_requested.avsc}.
 * Published to the {@code ride-events} topic when the Dispatch Service receives
 * a ride request from a rider. Triggers the driver assignment saga.
 *
 * @param tripId      UUID v4 identifying the Trip aggregate instance
 * @param riderId     identifier of the requesting rider (max 128 chars)
 * @param pickupLatitude  WGS-84 latitude of the pickup location (-90 to 90)
 * @param pickupLongitude WGS-84 longitude of the pickup location (-180 to 180)
 * @param requestedAt ISO 8601 UTC timestamp when the ride was requested
 */
public record TripRequestedPayload(
    String tripId,
    String riderId,
    double pickupLatitude,
    double pickupLongitude,
    String requestedAt
) {}
