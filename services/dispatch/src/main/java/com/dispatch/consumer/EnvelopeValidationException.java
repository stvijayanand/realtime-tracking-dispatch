package com.dispatch.consumer;

/**
 * Thrown by {@link EnvelopeValidator} when a consumed Kafka message envelope
 * fails validation. Identifies the failing field by name.
 */
public class EnvelopeValidationException extends RuntimeException {

    private final String field;

    public EnvelopeValidationException(String field, String message) {
        super(String.format("Envelope validation failed: field '%s': %s", field, message));
        this.field = field;
    }

    public String getField() {
        return field;
    }
}
