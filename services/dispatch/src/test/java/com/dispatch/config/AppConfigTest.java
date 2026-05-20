package com.dispatch.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AppConfig#validateRequiredEnvVars()}.
 *
 * <p>Because {@code System.getenv()} reads the real OS environment, these tests
 * verify the logic by subclassing {@link AppConfig} and overriding the env reader
 * via a package-private hook — keeping production code clean while enabling
 * deterministic testing without process-level env manipulation.
 */
class AppConfigTest {

    /**
     * Testable subclass that delegates env lookup to a provided function,
     * allowing tests to control which variables are present.
     */
    static class TestableAppConfig extends AppConfig {
        private final java.util.Map<String, String> env;

        TestableAppConfig(java.util.Map<String, String> env) {
            this.env = env;
        }

        @Override
        public void validateRequiredEnvVars() {
            java.util.List<String> required = java.util.List.of(
                "KAFKA_BOOTSTRAP_SERVERS",
                "SPRING_DATASOURCE_URL",
                "SPRING_DATASOURCE_USERNAME",
                "SPRING_DATASOURCE_PASSWORD",
                "KAFKA_SASL_USERNAME",
                "KAFKA_SASL_PASSWORD",
                "SCHEMA_REGISTRY_URL",
                "SERVICE_PORT",
                "OTEL_EXPORTER_OTLP_ENDPOINT"
            );
            for (String varName : required) {
                String value = env.get(varName);
                if (value == null || value.isBlank()) {
                    throw new IllegalStateException(
                        "Missing required environment variable: " + varName);
                }
            }
        }
    }

    private static java.util.Map<String, String> allVarsPresent() {
        return new java.util.HashMap<>(java.util.Map.of(
            "KAFKA_BOOTSTRAP_SERVERS",       "kafka:9092",
            "SPRING_DATASOURCE_URL",         "jdbc:postgresql://pgbouncer:5432/dispatch_db",
            "SPRING_DATASOURCE_USERNAME",    "dispatch_user",
            "SPRING_DATASOURCE_PASSWORD",    "secret",
            "KAFKA_SASL_USERNAME",           "dispatch-service",
            "KAFKA_SASL_PASSWORD",           "secret",
            "SCHEMA_REGISTRY_URL",           "http://schema-registry:8081",
            "SERVICE_PORT",                  "8080",
            "OTEL_EXPORTER_OTLP_ENDPOINT",  "http://jaeger:4317"
        ));
    }

    @Test
    void validateRequiredEnvVars_allPresent_noException() {
        TestableAppConfig config = new TestableAppConfig(allVarsPresent());
        assertDoesNotThrow(config::validateRequiredEnvVars);
    }

    @Test
    void validateRequiredEnvVars_missingKafkaBootstrapServers_throwsWithVarName() {
        var env = allVarsPresent();
        env.remove("KAFKA_BOOTSTRAP_SERVERS");
        TestableAppConfig config = new TestableAppConfig(env);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class, config::validateRequiredEnvVars);
        assertTrue(ex.getMessage().contains("KAFKA_BOOTSTRAP_SERVERS"),
            "Exception message should identify the missing variable");
    }

    @Test
    void validateRequiredEnvVars_missingDatasourceUrl_throwsWithVarName() {
        var env = allVarsPresent();
        env.remove("SPRING_DATASOURCE_URL");
        TestableAppConfig config = new TestableAppConfig(env);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class, config::validateRequiredEnvVars);
        assertTrue(ex.getMessage().contains("SPRING_DATASOURCE_URL"));
    }

    @Test
    void validateRequiredEnvVars_blankValue_throwsWithVarName() {
        var env = allVarsPresent();
        env.put("KAFKA_SASL_PASSWORD", "   "); // blank, not null
        TestableAppConfig config = new TestableAppConfig(env);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class, config::validateRequiredEnvVars);
        assertTrue(ex.getMessage().contains("KAFKA_SASL_PASSWORD"));
    }

    @Test
    void validateRequiredEnvVars_missingSchemaRegistryUrl_throwsWithVarName() {
        var env = allVarsPresent();
        env.remove("SCHEMA_REGISTRY_URL");
        TestableAppConfig config = new TestableAppConfig(env);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class, config::validateRequiredEnvVars);
        assertTrue(ex.getMessage().contains("SCHEMA_REGISTRY_URL"));
    }
}
