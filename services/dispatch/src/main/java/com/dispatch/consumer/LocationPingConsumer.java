package com.dispatch.consumer;

import com.dispatch.events.DomainEventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka consumer for the {@code gps-pings} topic, consumer group
 * {@code dispatch-location-group}.
 *
 * <p>This is the CQRS local read model stub for the Dispatch Service (ADR 005).
 * In Phase 1, it validates the envelope and logs receipt at DEBUG level.
 * In Phase 2, it will write driver locations to Redis via {@code GEOADD}.
 *
 * <p>On deserialisation failure or envelope validation failure: logs a WARNING
 * to stderr and continues consuming — never crashes (Requirement 3.15).
 */
@Component
public class LocationPingConsumer {

    private static final Logger log = LoggerFactory.getLogger(LocationPingConsumer.class);
    private static final String LOCATION_PING_RECEIVED = "LocationPingReceived";

    @KafkaListener(
        topics = "${KAFKA_TOPIC_GPS_PINGS:gps-pings}",
        groupId = "dispatch-location-group",
        containerFactory = "locationListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, Object> record) {
        try {
            if (!(record.value() instanceof Map)) {
                log.warn("Received non-map message on gps-pings, skipping. offset={}",
                    record.offset());
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> raw = (Map<String, Object>) record.value();

            // Build a DomainEventEnvelope for validation.
            String eventId   = (String) raw.get("event_id");
            String eventType = (String) raw.get("event_type");

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = raw.get("payload") instanceof Map
                ? (Map<String, Object>) raw.get("payload")
                : null;

            DomainEventEnvelope envelope = new DomainEventEnvelope(
                eventId, eventType, (String) raw.get("occurred_at"), payload);

            // Validate envelope — throws EnvelopeValidationException on failure.
            EnvelopeValidator.validate(envelope);

            // Phase 1: log at DEBUG level only.
            // Phase 2: call redisTemplate.opsForGeo().add(...) here.
            log.debug("LocationPingReceived: eventId={} driverId={}",
                envelope.eventId(),
                payload != null ? payload.get("driver_id") : "unknown");

        } catch (EnvelopeValidationException e) {
            // Log warning and continue — do not crash the consumer (Requirement 3.15).
            log.warn("Envelope validation failed on gps-pings: {} offset={}",
                e.getMessage(), record.offset());
        } catch (Exception e) {
            // Catch-all for deserialisation or unexpected errors.
            log.warn("Failed to process gps-pings message at offset={}: {}",
                record.offset(), e.getMessage());
        }
    }
}
