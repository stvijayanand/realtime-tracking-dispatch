package com.dispatch.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

/**
 * WGS-84 geographic coordinate representing a rider's requested pickup point.
 */
public record PickupLocation(
    @JsonProperty("latitude")
    @NotNull
    @DecimalMin(value = "-90.0",  message = "latitude must be >= -90")
    @DecimalMax(value = "90.0",   message = "latitude must be <= 90")
    Double latitude,

    @JsonProperty("longitude")
    @NotNull
    @DecimalMin(value = "-180.0", message = "longitude must be >= -180")
    @DecimalMax(value = "180.0",  message = "longitude must be <= 180")
    Double longitude
) {}
