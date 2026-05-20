package com.dispatch.service;

import com.dispatch.domain.IllegalStateTransitionException;
import com.dispatch.domain.Trip;
import com.dispatch.domain.TripRepository;
import com.dispatch.domain.TripStatus;
import com.dispatch.events.DomainEventEnvelope;
import com.dispatch.web.dto.PickupLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DispatchServiceTest {

    private TripRepository tripRepository;
    private DriverSelectionStrategy driverSelectionStrategy;
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, Object> kafkaTemplate;
    private DispatchService dispatchService;

    private static final String RIDE_EVENTS_TOPIC = "ride-events";
    private static final PickupLocation PICKUP = new PickupLocation(51.5074, -0.1278);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        tripRepository = mock(TripRepository.class);
        driverSelectionStrategy = mock(DriverSelectionStrategy.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        dispatchService = new DispatchService(
            tripRepository, driverSelectionStrategy, kafkaTemplate, RIDE_EVENTS_TOPIC);

        when(driverSelectionStrategy.selectDriver(any())).thenReturn("driver-001");
        when(tripRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    // ── requestRide ───────────────────────────────────────────────────────────

    @Test
    void requestRide_persistsTripWithRequestedStatus() {
        dispatchService.requestRide("rider-1", PICKUP);

        ArgumentCaptor<Trip> captor = ArgumentCaptor.forClass(Trip.class);
        verify(tripRepository).save(captor.capture());

        Trip saved = captor.getValue();
        assertEquals(TripStatus.REQUESTED, saved.getStatus());
        assertEquals("rider-1", saved.getRiderId());
        assertNotNull(saved.getTripId());
    }

    @Test
    void requestRide_publishesTripRequestedEvent() {
        dispatchService.requestRide("rider-1", PICKUP);

        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(RIDE_EVENTS_TOPIC), any(String.class), valueCaptor.capture());

        DomainEventEnvelope envelope = (DomainEventEnvelope) valueCaptor.getValue();
        assertEquals("TripRequested", envelope.eventType());
        assertNotNull(envelope.eventId());
        assertNotNull(envelope.payload().get("trip_id"));
        assertEquals("rider-1", envelope.payload().get("rider_id"));
    }

    @Test
    void requestRide_returnsTripId() {
        UUID tripId = dispatchService.requestRide("rider-1", PICKUP);

        assertNotNull(tripId);
        // Verify the returned tripId matches what was saved
        ArgumentCaptor<Trip> captor = ArgumentCaptor.forClass(Trip.class);
        verify(tripRepository).save(captor.capture());
        assertEquals(captor.getValue().getTripId(), tripId);
    }

    // ── assignDriver ──────────────────────────────────────────────────────────

    @Test
    void assignDriver_updatesStatusToAssigned() {
        UUID tripId = UUID.randomUUID();
        Trip existingTrip = new Trip(tripId, "rider-1", TripStatus.REQUESTED,
            51.5, -0.1, Instant.now());
        when(tripRepository.findById(tripId)).thenReturn(Optional.of(existingTrip));

        dispatchService.assignDriver(tripId, "rider-1");

        assertEquals(TripStatus.ASSIGNED, existingTrip.getStatus());
        assertEquals("driver-001", existingTrip.getDriverId());
        assertNotNull(existingTrip.getAssignedAt());
    }

    @Test
    void assignDriver_publishesTripAssignedEvent() {
        UUID tripId = UUID.randomUUID();
        Trip existingTrip = new Trip(tripId, "rider-1", TripStatus.REQUESTED,
            51.5, -0.1, Instant.now());
        when(tripRepository.findById(tripId)).thenReturn(Optional.of(existingTrip));

        dispatchService.assignDriver(tripId, "rider-1");

        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(RIDE_EVENTS_TOPIC), any(String.class), valueCaptor.capture());

        DomainEventEnvelope envelope = (DomainEventEnvelope) valueCaptor.getValue();
        assertEquals("TripAssigned", envelope.eventType());
        assertEquals(tripId.toString(), envelope.payload().get("trip_id"));
        assertEquals("driver-001", envelope.payload().get("driver_id"));
        assertEquals("rider-1", envelope.payload().get("rider_id"));
    }

    @Test
    void assignDriver_throwsOnInvalidTransition_alreadyAssigned() {
        UUID tripId = UUID.randomUUID();
        Trip alreadyAssigned = new Trip(tripId, "rider-1", TripStatus.ASSIGNED,
            51.5, -0.1, Instant.now());
        when(tripRepository.findById(tripId)).thenReturn(Optional.of(alreadyAssigned));

        assertThrows(IllegalStateTransitionException.class,
            () -> dispatchService.assignDriver(tripId, "rider-1"));

        // Kafka must NOT be called on invalid transition
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void assignDriver_throwsWhenTripNotFound() {
        UUID tripId = UUID.randomUUID();
        when(tripRepository.findById(tripId)).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class,
            () -> dispatchService.assignDriver(tripId, "rider-1"));
    }
}
