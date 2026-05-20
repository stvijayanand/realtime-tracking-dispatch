package com.dispatch.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "trips")
public class Trip {

    @Id
    @Column(name = "trip_id", nullable = false, updatable = false)
    private UUID tripId;

    @Column(name = "rider_id", nullable = false, length = 128)
    private String riderId;

    @Column(name = "driver_id", length = 128)
    private String driverId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private TripStatus status;

    @Column(name = "pickup_lat")
    private Double pickupLat;

    @Column(name = "pickup_lng")
    private Double pickupLng;

    @Column(name = "requested_at", nullable = false)
    private Instant requestedAt;

    @Column(name = "assigned_at")
    private Instant assignedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Trip() {}

    public Trip(UUID tripId, String riderId, TripStatus status,
                Double pickupLat, Double pickupLng, Instant requestedAt) {
        this.tripId = tripId;
        this.riderId = riderId;
        this.status = status;
        this.pickupLat = pickupLat;
        this.pickupLng = pickupLng;
        this.requestedAt = requestedAt;
        this.updatedAt = Instant.now();
    }

    @PrePersist
    protected void onCreate() {
        if (updatedAt == null) updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    // Getters
    public UUID getTripId() { return tripId; }
    public String getRiderId() { return riderId; }
    public String getDriverId() { return driverId; }
    public TripStatus getStatus() { return status; }
    public Double getPickupLat() { return pickupLat; }
    public Double getPickupLng() { return pickupLng; }
    public Instant getRequestedAt() { return requestedAt; }
    public Instant getAssignedAt() { return assignedAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    // Setters
    public void setDriverId(String driverId) { this.driverId = driverId; }
    public void setStatus(TripStatus status) { this.status = status; }
    public void setAssignedAt(Instant assignedAt) { this.assignedAt = assignedAt; }
}
