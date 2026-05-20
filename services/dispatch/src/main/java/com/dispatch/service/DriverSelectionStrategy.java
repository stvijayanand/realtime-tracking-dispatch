package com.dispatch.service;

import com.dispatch.web.dto.PickupLocation;

/**
 * Strategy interface for selecting a driver to assign to a trip.
 *
 * <p>Phase 1: implemented by {@link HardcodedDriverSelectionStrategy} — round-robin
 * from a static in-memory list, ignoring the pickup location.
 *
 * <p>Phase 2: replaced by a Redis GEORADIUS-backed implementation that queries the
 * CQRS local read model (driver locations indexed via {@code GEOADD}) to find the
 * nearest available driver (see ADR 005).
 */
public interface DriverSelectionStrategy {

    /**
     * Selects a driver identifier for the given pickup location.
     *
     * @param pickup the rider's requested pickup coordinate
     * @return a non-null, non-empty driver identifier string
     */
    String selectDriver(PickupLocation pickup);
}
