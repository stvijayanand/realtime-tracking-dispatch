package com.dispatch.web.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

/**
 * WGS-84 geographic coordinate representing a rider's requested pickup point.
 * Used in the {@code POST /request-ride} request body and passed to the
 * {@code DriverSelectionStrategy} for nearest-driver matching.
 */
public record PickupLocation(
    @NotNull
    @DecimalMin(value = "-90.0", message = "latitude must be >= -90")
    @DecimalMax(value = "90.0",  message = "latitude must be <= 90")
    Double latitude,

    @NotNull
    @DecimalMin(value = "-180.0", message = "longitude must be >= -180")
    @DecimalMax(value = "180.0",  message = "longitude must be <= 180")
    Double longitude
) {}
