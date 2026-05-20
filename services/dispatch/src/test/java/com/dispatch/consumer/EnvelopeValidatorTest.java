package com.dispatch.consumer;

import com.dispatch.events.DomainEventEnvelope;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeValidatorTest {

    private static DomainEventEnvelope valid() {
        return new DomainEventEnvelope(
            UUID.randomUUID().toString(),
            "LocationPingReceived",
            "2024-01-15T10:30:00Z",
            Map.of("driver_id", "driver-001")
        );
    }

    @Test
    void validate_validEnvelope_noException() {
        assertDoesNotThrow(() -> EnvelopeValidator.validate(valid()));
    }

    @Test
    void validate_nullEventId_throwsWithFieldName() {
        DomainEventEnvelope env = new DomainEventEnvelope(
            null, "LocationPingReceived", "2024-01-15T10:30:00Z", Map.of());
        EnvelopeValidationException ex = assertThrows(
            EnvelopeValidationException.class, () -> EnvelopeValidator.validate(env));
        assertEquals("eventId", ex.getField());
    }

    @Test
    void validate_blankEventId_throwsWithFieldName() {
        DomainEventEnvelope env = new DomainEventEnvelope(
            "   ", "LocationPingReceived", "2024-01-15T10:30:00Z", Map.of());
        EnvelopeValidationException ex = assertThrows(
            EnvelopeValidationException.class, () -> EnvelopeValidator.validate(env));
        assertEquals("eventId", ex.getField());
    }

    @Test
    void validate_invalidUuidEventId_throwsWithFieldName() {
        DomainEventEnvelope env = new DomainEventEnvelope(
            "not-a-uuid", "LocationPingReceived", "2024-01-15T10:30:00Z", Map.of());
        EnvelopeValidationException ex = assertThrows(
            EnvelopeValidationException.class, () -> EnvelopeValidator.validate(env));
        assertEquals("eventId", ex.getField());
    }

    @Test
    void validate_nullEventType_throwsWithFieldName() {
        DomainEventEnvelope env = new DomainEventEnvelope(
            UUID.randomUUID().toString(), null, "2024-01-15T10:30:00Z", Map.of());
        EnvelopeValidationException ex = assertThrows(
            EnvelopeValidationException.class, () -> EnvelopeValidator.validate(env));
        assertEquals("eventType", ex.getField());
    }

    @Test
    void validate_blankEventType_throwsWithFieldName() {
        DomainEventEnvelope env = new DomainEventEnvelope(
            UUID.randomUUID().toString(), "", "2024-01-15T10:30:00Z", Map.of());
        EnvelopeValidationException ex = assertThrows(
            EnvelopeValidationException.class, () -> EnvelopeValidator.validate(env));
        assertEquals("eventType", ex.getField());
    }
}
