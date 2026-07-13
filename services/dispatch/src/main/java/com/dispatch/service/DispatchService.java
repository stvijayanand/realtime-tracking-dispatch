package com.dispatch.service;

import com.dispatch.domain.Trip;
import com.dispatch.domain.TripRepository;
import com.dispatch.domain.TripStatus;
import com.dispatch.events.AvroEventBuilder;
import com.dispatch.events.DomainEventEnvelope;
import com.dispatch.events.EventEnvelopeFactory;
import com.dispatch.web.dto.PickupLocation;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Core service orchestrating the Trip lifecycle for the Dispatch bounded context.
 *
 * <p>Owns the Trip aggregate — all state transitions go through this service.
 * UUID generation is delegated to {@link EventEnvelopeFactory}; this service
 * never calls {@code UUID.randomUUID()} directly (except for the tripId, which
 * is a domain concern, not an infrastructure concern).
 *
 * <p>Kafka publish uses {@code KafkaTemplate} configured with Avro serialization
 * via Schema Registry, {@code acks=all}, and {@code enable.idempotence=true}
 * (Requirement 3.10).
 */
@Service
public class DispatchService {

    private final TripRepository tripRepository;
    private final DriverSelectionStrategy driverSelectionStrategy;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String rideEventsTopic;

    public DispatchService(
            TripRepository tripRepository,
            DriverSelectionStrategy driverSelectionStrategy,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${KAFKA_TOPIC_RIDE_EVENTS:ride-events}") String rideEventsTopic) {
        this.tripRepository = tripRepository;
        this.driverSelectionStrategy = driverSelectionStrategy;
        this.kafkaTemplate = kafkaTemplate;
        this.rideEventsTopic = rideEventsTopic;
    }

    /**
     * Handles a ride request from a rider.
     *
     * <ol>
     *   <li>Generates a new {@code tripId} (UUID v4).</li>
     *   <li>Persists {@code Trip(status=REQUESTED)} to PostgreSQL via PgBouncer.</li>
     *   <li>Builds a {@code TripRequested} envelope via {@link EventEnvelopeFactory}.</li>
     *   <li>Converts to Avro {@link GenericRecord} and publishes to {@code ride-events}.</li>
     *   <li>Returns the {@code tripId} in the HTTP 202 response.</li>
     * </ol>
     *
     * @param riderId identifier of the requesting rider
     * @param pickup  WGS-84 coordinate of the pickup location
     * @return the generated {@code tripId}
     */
    @Transactional
    public UUID requestRide(String riderId, PickupLocation pickup) {
        UUID tripId = UUID.randomUUID();
        Instant requestedAt = Instant.now();

        Trip trip = new Trip(
            tripId, riderId, TripStatus.REQUESTED,
            pickup.latitude(), pickup.longitude(), requestedAt);
        tripRepository.save(trip);

        DomainEventEnvelope envelope = EventEnvelopeFactory.buildTripRequested(
            tripId, riderId, pickup.latitude(), pickup.longitude(), requestedAt);

        GenericRecord avroRecord = AvroEventBuilder.buildTripRequestedRecord(envelope);
        kafkaTemplate.send(rideEventsTopic, tripId.toString(), avroRecord);

        return tripId;
    }

    /**
     * Assigns a driver to an existing trip.
     *
     * <ol>
     *   <li>Loads the Trip from PostgreSQL; throws if not found.</li>
     *   <li>Asserts the state transition {@code REQUESTED → ASSIGNED} is valid.</li>
     *   <li>Selects a driver via {@link DriverSelectionStrategy}.</li>
     *   <li>Updates Trip ({@code status=ASSIGNED}, {@code driverId}, {@code assignedAt}).</li>
     *   <li>Builds a {@code TripAssigned} envelope and publishes Avro record to Kafka.</li>
     * </ol>
     *
     * @param tripId  UUID of the Trip aggregate to assign
     * @param riderId identifier of the rider (carried forward from TripRequested)
     */
    @Transactional
    public void assignDriver(UUID tripId, String riderId) {
        Trip trip = tripRepository.findById(tripId)
            .orElseThrow(() -> new IllegalArgumentException("Trip not found: " + tripId));

        // Guard: throws IllegalStateTransitionException on invalid transition.
        trip.getStatus().assertCanTransitionTo(TripStatus.ASSIGNED);

        PickupLocation pickup = new PickupLocation(trip.getPickupLat(), trip.getPickupLng());
        String driverId = driverSelectionStrategy.selectDriver(pickup);

        Instant assignedAt = Instant.now();
        trip.setDriverId(driverId);
        trip.setStatus(TripStatus.ASSIGNED);
        trip.setAssignedAt(assignedAt);
        tripRepository.save(trip);

        DomainEventEnvelope envelope = EventEnvelopeFactory.buildTripAssigned(
            tripId, driverId, riderId, assignedAt);

        GenericRecord avroRecord = AvroEventBuilder.buildTripAssignedRecord(envelope);
        kafkaTemplate.send(rideEventsTopic, tripId.toString(), avroRecord);
    }
}
