package com.dispatch.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
 *   <li>{@code rideEventsConsumerFactory} — {@code ride-events} topic, Avro deserialiser
 *       via Schema Registry, group {@code dispatch-consumer-group}</li>
 *   <li>{@code locationConsumerFactory} — {@code gps-pings} topic, Avro deserialiser
 *       via Schema Registry, group {@code dispatch-location-group}</li>
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

    @Value("${SCHEMA_REGISTRY_URL:http://schema-registry:8081}")
    private String schemaRegistryUrl;

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

    // ── ride-events consumer: Avro deserialiser via Schema Registry ────────────

    /**
     * Consumer factory for the {@code ride-events} topic.
     * Uses Avro deserialization via Schema Registry. The Dispatch Service
     * publishes and consumes Avro-encoded Domain Events.
     * Wrapped in {@link ErrorHandlingDeserializer} for resilience.
     */
    @Bean("rideEventsConsumerFactory")
    public ConsumerFactory<String, Object> rideEventsConsumerFactory() {
        Map<String, Object> props = saslProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dispatch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Avro deserialiser via Schema Registry, wrapped in ErrorHandlingDeserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put("value.subject.name.strategy",
            "io.confluent.kafka.serializers.subject.RecordNameStrategy");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── gps-pings consumer: Avro deserialiser via Schema Registry ──────────────

    /**
     * Consumer factory for the {@code gps-pings} topic.
     *
     * <p>Uses Avro deserialization via Schema Registry. The Ingest Service
     * publishes Avro-encoded {@code LocationPingReceived} events.
     * Wrapped in {@link ErrorHandlingDeserializer} for resilience.
     */
    @Bean("locationConsumerFactory")
    public ConsumerFactory<String, Object> locationConsumerFactory() {
        Map<String, Object> props = saslProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dispatch-location-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Avro deserialiser via Schema Registry, wrapped in ErrorHandlingDeserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put("value.subject.name.strategy",
            "io.confluent.kafka.serializers.subject.RecordNameStrategy");

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
