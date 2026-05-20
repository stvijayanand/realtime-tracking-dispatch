package com.dispatch.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Application configuration with fail-fast startup validation.
 *
 * <p>Validates that all required environment variables are present before the
 * application context finishes loading. If any required variable is absent or
 * blank, throws {@link IllegalStateException} identifying the missing variable
 * so the service never starts with incomplete configuration (Requirement 10.10).
 */
@Configuration
public class AppConfig {

    private static final List<String> REQUIRED_ENV_VARS = List.of(
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

    @PostConstruct
    public void validateRequiredEnvVars() {
        for (String varName : REQUIRED_ENV_VARS) {
            String value = System.getenv(varName);
            if (value == null || value.isBlank()) {
                throw new IllegalStateException(
                    "Missing required environment variable: " + varName);
            }
        }
    }
}
