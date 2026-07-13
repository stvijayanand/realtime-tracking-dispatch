package com.dispatch.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for the {@code gps-pings} topic, consumer group
 * {@code dispatch-location-group}.
 *
 * <p>This is the CQRS local read model stub for the Dispatch Service (ADR 005).
 * In Phase 1, it validates the envelope and logs receipt at DEBUG level.
 * In Phase 2, it will write driver locations to Redis via {@code GEOADD}.
 *
 * <p>Messages are Avro-encoded {@link GenericRecord} instances deserialized by
 * the {@code KafkaAvroDeserializer} via Schema Registry.
 *
 * <p>On deserialisation failure or envelope validation failure: logs a WARNING
 * to stderr and continues consuming — never crashes (Requirement 3.15).
 */
@Component
public class LocationPingConsumer {

    private static final Logger log = LoggerFactory.getLogger(LocationPingConsumer.class);

    @KafkaListener(
        topics = "${KAFKA_TOPIC_GPS_PINGS:gps-pings}",
        groupId = "dispatch-location-group",
        containerFactory = "locationListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, Object> record) {
        try {
            // Value is an Avro GenericRecord deserialized by KafkaAvroDeserializer.
            GenericRecord envelope = (GenericRecord) record.value();
            String eventType = envelope.get("event_type") != null
                ? envelope.get("event_type").toString() : "unknown";
            // Log at DEBUG only — this fires at 10+ pings/sec per driver.
            // Phase 2: extract driver location from payload, write to Redis GEOADD.
            log.debug("LocationPingReceived: offset={} key={} event_type={}",
                record.offset(), record.key(), eventType);

        } catch (Exception e) {
            log.warn("Failed to process gps-pings message at offset={}: {}",
                record.offset(), e.getMessage());
        }
    }
}
