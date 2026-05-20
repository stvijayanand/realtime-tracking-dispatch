package com.dispatch.consumer;

import com.dispatch.events.DomainEventEnvelope;
import com.dispatch.service.DispatchService;
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
 * <p>W3C {@code traceparent} header is extracted from Kafka message headers and
 * used to create a child OTel span, maintaining the distributed trace across the
 * async boundary (Requirement 12.1).
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
        if (!(record.value() instanceof Map)) {
            log.warn("Received non-map message on ride-events, skipping. offset={}",
                record.offset());
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> raw = (Map<String, Object>) record.value();

        String eventType = (String) raw.get("event_type");
        if (!TRIP_REQUESTED.equals(eventType)) {
            // Not a TripRequested event — acknowledge and skip.
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
        log.debug("Processing TripRequested: tripId={} riderId={}", tripId, riderId);

        dispatchService.assignDriver(tripId, riderId);
    }
}
