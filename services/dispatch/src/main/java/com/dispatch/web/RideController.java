package com.dispatch.web;

import com.dispatch.service.DispatchService;
import com.dispatch.web.dto.RequestRideRequest;
import com.dispatch.web.dto.RequestRideResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * REST controller for ride request operations.
 *
 * <p>Enforces a 64 KB maximum request body size via Spring's
 * {@code spring.servlet.multipart.max-request-size} / {@code server.tomcat.max-http-form-post-size}
 * configuration in {@code application.yml}. Returns HTTP 413 for oversized bodies,
 * HTTP 422 for validation failures (Requirement 3.11, 10.8, 10.9).
 */
@RestController
@Tag(name = "Ride", description = "Ride request and dispatch operations")
public class RideController {

    private final DispatchService dispatchService;

    public RideController(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    /**
     * Submits a ride request. Publishes a {@code TripRequested} Domain Event and
     * returns the generated {@code trip_id}.
     */
    @Operation(summary = "Request a ride",
               description = "Publishes a TripRequested Domain Event and returns the trip_id")
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Ride request accepted",
            content = @Content(schema = @Schema(implementation = RequestRideResponse.class))),
        @ApiResponse(responseCode = "413", description = "Request body exceeds 64 KB"),
        @ApiResponse(responseCode = "422", description = "Validation error")
    })
    @PostMapping("/request-ride")
    public ResponseEntity<RequestRideResponse> requestRide(
            @Valid @RequestBody RequestRideRequest request) {

        UUID tripId = dispatchService.requestRide(
            request.riderId(), request.pickupLocation());

        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(new RequestRideResponse(tripId));
    }
}
