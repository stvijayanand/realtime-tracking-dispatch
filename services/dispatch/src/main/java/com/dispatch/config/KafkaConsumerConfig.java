package com.dispatch.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the Dispatch Service.
 *
 * <p>Two consumer factories:
 * <ul>
 *   <li>{@code rideEventsConsumerFactory} — {@code ride-events} topic, Avro deserialiser,
 *       group {@code dispatch-consumer-group}</li>
 *   <li>{@code locationConsumerFactory} — {@code gps-pings} topic, plain String deserialiser
 *       (Phase 1 stub — Ingest publishes JSON, not Avro), group {@code dispatch-location-group}</li>
 * </ul>
 *
 * <p>Both factories wrap the value deserialiser in {@link ErrorHandlingDeserializer} so that
 * a malformed message logs a warning and commits the offset rather than crashing the consumer
 * thread (Requirement 3.14, 3.15).
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String bootstrapServers;

    @Value("${KAFKA_SASL_USERNAME}")
    private String saslUsername;

    @Value("${KAFKA_SASL_PASSWORD}")
    private String saslPassword;

    // ── SASL/PLAIN base props ──────────────────────────────────────────────────

    private Map<String, Object> saslProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + saslUsername + "\" " +
            "password=\"" + saslPassword + "\";");
        return props;
    }

    // ── ride-events consumer: Avro deserialiser ────────────────────────────────

    /**
     * Consumer factory for the {@code ride-events} topic.
     * Uses plain String deserialisation — the Dispatch Service publishes JSON
     * (not Avro) in Phase 1. The RideEventsConsumer parses the JSON manually.
     * Wrapped in {@link ErrorHandlingDeserializer} for resilience.
     */
    @Bean("rideEventsConsumerFactory")
    public ConsumerFactory<String, Object> rideEventsConsumerFactory() {
        Map<String, Object> props = saslProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dispatch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Plain String deserialiser — Dispatch publishes JSON, not Avro, in Phase 1.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── gps-pings consumer: plain String deserialiser (Phase 1 stub) ──────────

    /**
     * Consumer factory for the {@code gps-pings} topic.
     *
     * <p>Phase 1: the Ingest Service publishes plain JSON (not Avro). The Dispatch
     * service only stubs this consumer — it logs receipt at DEBUG level and does NOT
     * write to Redis (that is Phase 2). Using {@link StringDeserializer} avoids the
     * "Unknown magic byte" error that occurs when the Avro deserialiser encounters
     * a non-Avro payload.
     *
     * <p>Phase 2: switch to {@link KafkaAvroDeserializer} once the Ingest Service
     * publishes Avro-encoded {@code LocationPingReceived} events.
     */
    @Bean("locationConsumerFactory")
    public ConsumerFactory<String, Object> locationConsumerFactory() {
        Map<String, Object> props = saslProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dispatch-location-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Plain String deserialiser — Phase 1 Ingest publishes JSON, not Avro.
        // Wrapped in ErrorHandlingDeserializer for resilience.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── Listener container factories ──────────────────────────────────────────

    @Bean("rideEventsListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> rideEventsListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(rideEventsConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(buildErrorHandler());
        return factory;
    }

    @Bean("locationListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> locationListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(locationConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(buildErrorHandler());
        return factory;
    }

    /**
     * Exponential backoff error handler: 1s → 2s → 4s → ... → 30s cap.
     * Spring Kafka 3.x replacement for the removed setRetryTemplate() API.
     */
    private DefaultErrorHandler buildErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        backOff.setMaxElapsedTime(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME);
        return new DefaultErrorHandler(backOff);
    }
}
