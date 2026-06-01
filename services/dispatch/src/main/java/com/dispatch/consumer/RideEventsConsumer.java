package com.dispatch.consumer;

import com.dispatch.service.DispatchService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
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
 * <p>Messages are plain JSON strings (Phase 1). The value deserialiser is
 * {@code StringDeserializer} — this consumer parses the JSON manually.
 */
@Component
public class RideEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(RideEventsConsumer.class);
    private static final String TRIP_REQUESTED = "TripRequested";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

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

        // Value is a plain JSON String (StringDeserializer, Phase 1).
        Map<String, Object> raw;
        try {
            String json = record.value().toString();
            raw = MAPPER.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            log.warn("Failed to parse JSON on ride-events, skipping. offset={} error={}",
                record.offset(), e.getMessage());
            return;
        }

        String eventType = (String) raw.get("event_type");
        if (!TRIP_REQUESTED.equals(eventType)) {
            log.debug("Skipping event_type={} on ride-events", eventType);
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) raw.get("payload");
        if (payload == null) {
            log.warn("TripRequested event missing payload, skipping. offset={}", record.offset());
            return;
        }

        String tripIdStr = (String) payload.get("trip_id");
        String riderId   = (String) payload.get("rider_id");

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
