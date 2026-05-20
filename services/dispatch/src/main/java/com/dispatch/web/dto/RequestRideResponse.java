package com.dispatch.web.dto;

import java.util.UUID;

/**
 * Response body for a successful {@code POST /request-ride} (HTTP 202).
 *
 * @param tripId UUID of the newly created Trip aggregate
 */
public record RequestRideResponse(UUID tripId) {}
