package com.dispatch.consumer;

import com.dispatch.events.DomainEventEnvelope;

/**
 * Stateless validator for {@link DomainEventEnvelope} instances consumed from Kafka.
 *
 * <p>Validates that the envelope has a non-empty {@code eventId} (UUID string) and
 * a non-null, non-empty {@code eventType}. Throws {@link EnvelopeValidationException}
 * identifying the failing field on any violation (Requirement 3.14, 3.15).
 */
public final class EnvelopeValidator {

    private EnvelopeValidator() {}

    /**
     * Validates the given envelope.
     *
     * @param envelope the envelope to validate
     * @throws EnvelopeValidationException if {@code eventId} is blank or not a valid UUID,
     *                                     or if {@code eventType} is blank
     */
    public static void validate(DomainEventEnvelope envelope) {
        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            throw new EnvelopeValidationException("eventId",
                "must be a non-empty UUID string");
        }

        // Validate UUID format
        try {
            java.util.UUID.fromString(envelope.eventId());
        } catch (IllegalArgumentException e) {
            throw new EnvelopeValidationException("eventId",
                "must be a valid UUID: " + envelope.eventId());
        }

        if (envelope.eventType() == null || envelope.eventType().isBlank()) {
            throw new EnvelopeValidationException("eventType",
                "must be non-null and non-empty");
        }
    }
}
