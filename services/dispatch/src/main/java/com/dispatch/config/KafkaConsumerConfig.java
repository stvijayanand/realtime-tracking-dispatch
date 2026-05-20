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
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the Dispatch Service.
 *
 * <p>Configures two consumer factories:
 * <ul>
 *   <li>{@code rideEventsConsumerFactory} — consumes from {@code ride-events} topic,
 *       group {@code dispatch-consumer-group}</li>
 *   <li>{@code locationConsumerFactory} — consumes from {@code gps-pings} topic,
 *       group {@code dispatch-location-group} (CQRS read model stub per ADR 005)</li>
 * </ul>
 *
 * <p>Both factories use Avro deserialisation via Confluent Schema Registry and
 * SASL/PLAIN authentication. Exponential backoff retry (up to 5 attempts) is
 * configured on startup connection failure (Requirement 3.5).
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String bootstrapServers;

    @Value("${KAFKA_SASL_USERNAME}")
    private String saslUsername;

    @Value("${KAFKA_SASL_PASSWORD}")
    private String saslPassword;

    @Value("${SCHEMA_REGISTRY_URL}")
    private String schemaRegistryUrl;

    /**
     * Builds the base consumer properties shared by both consumer factories.
     * SASL/PLAIN credentials and Avro deserialiser are configured here.
     */
    private Map<String, Object> baseConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Return generic Map<String,Object> rather than specific Avro generated classes.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        // SASL/PLAIN authentication (Phase 1: PLAINTEXT; Phase 2: SASL_SSL + SCRAM-SHA-512)
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + saslUsername + "\" " +
            "password=\"" + saslPassword + "\";");
        return props;
    }

    @Bean("rideEventsConsumerFactory")
    public ConsumerFactory<String, Object> rideEventsConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps("dispatch-consumer-group"));
    }

    @Bean("locationConsumerFactory")
    public ConsumerFactory<String, Object> locationConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps("dispatch-location-group"));
    }

    /**
     * Listener container factory for the {@code ride-events} topic.
     * Configured with exponential backoff retry (up to 5 attempts, 1s → 30s).
     */
    @Bean("rideEventsListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> rideEventsListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(rideEventsConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setRetryTemplate(buildRetryTemplate());
        return factory;
    }

    /**
     * Listener container factory for the {@code gps-pings} topic.
     * Configured with exponential backoff retry (up to 5 attempts, 1s → 30s).
     */
    @Bean("locationListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> locationListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(locationConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setRetryTemplate(buildRetryTemplate());
        return factory;
    }

    /**
     * Builds a RetryTemplate with exponential backoff: up to 5 attempts,
     * starting at 1 second, doubling each time, capped at 30 seconds.
     * Satisfies Requirement 3.5 (retry on startup connection failure).
     */
    private RetryTemplate buildRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(5);
        retryTemplate.setRetryPolicy(retryPolicy);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1_000L);   // 1 second
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(30_000L);       // 30 seconds
        retryTemplate.setBackOffPolicy(backOffPolicy);

        return retryTemplate;
    }
}
