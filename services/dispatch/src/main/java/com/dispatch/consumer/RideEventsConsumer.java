package com.dispatch.consumer;

import com.dispatch.service.DispatchService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka consumer for the {@code ride-events} topic, consumer group
 * {@code dispatch-consumer-group}.
 *
 * <p>Filters for {@code event_type == "TripRequested"} only. All other event
 * types are acknowledged and skipped (Requirement 3.1). Delegates to
 * {@link DispatchService#assignDriver(UUID, String)} which must complete within
 * 2 seconds (Requirement 3.7).
 *
 * <p>Messages are Avro-encoded {@link GenericRecord} instances deserialized by
 * the {@code KafkaAvroDeserializer} via Schema Registry.
 */
@Component
public class RideEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(RideEventsConsumer.class);
    private static final String TRIP_REQUESTED = "TripRequested";

    private final DispatchService dispatchService;

    public RideEventsConsumer(DispatchService dispatchService) {
        this.dispatchService = dispatchService;
    }

    @KafkaListener(
        topics = "${KAFKA_TOPIC_RIDE_EVENTS:ride-events}",
        groupId = "dispatch-consumer-group",
        containerFactory = "rideEventsListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, Object> record) {
        if (record.value() == null) {
            log.warn("Received null message on ride-events, skipping. offset={}", record.offset());
            return;
        }

        // Value is an Avro GenericRecord deserialized by KafkaAvroDeserializer.
        GenericRecord envelope;
        try {
            envelope = (GenericRecord) record.value();
        } catch (ClassCastException e) {
            log.warn("Failed to cast value to GenericRecord on ride-events, skipping. offset={} error={}",
                record.offset(), e.getMessage());
            return;
        }

        String eventType = envelope.get("event_type") != null
            ? envelope.get("event_type").toString() : null;
        if (!TRIP_REQUESTED.equals(eventType)) {
            log.debug("Skipping event_type={} on ride-events", eventType);
            return;
        }

        GenericRecord payload = (GenericRecord) envelope.get("payload");
        if (payload == null) {
            log.warn("TripRequested event missing payload, skipping. offset={}", record.offset());
            return;
        }

        String tripIdStr = payload.get("trip_id") != null
            ? payload.get("trip_id").toString() : null;
        String riderId = payload.get("rider_id") != null
            ? payload.get("rider_id").toString() : null;

        if (tripIdStr == null || riderId == null) {
            log.warn("TripRequested payload missing trip_id or rider_id, skipping. offset={}",
                record.offset());
            return;
        }

        UUID tripId = UUID.fromString(tripIdStr);
        log.info("Processing TripRequested: tripId={} riderId={}", tripId, riderId);

        dispatchService.assignDriver(tripId, riderId);
    }
}
