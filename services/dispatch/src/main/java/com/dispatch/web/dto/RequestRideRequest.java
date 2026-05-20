package com.dispatch.web.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Request body for {@code POST /request-ride}.
 *
 * @param riderId        identifier of the requesting rider; non-blank, max 128 chars
 * @param pickupLocation WGS-84 coordinate of the pickup point; non-null, validated
 */
public record RequestRideRequest(
    @NotBlank(message = "rider_id must not be blank")
    @Size(max = 128, message = "rider_id must not exceed 128 characters")
    String riderId,

    @NotNull(message = "pickup_location must not be null")
    @Valid
    PickupLocation pickupLocation
) {}
