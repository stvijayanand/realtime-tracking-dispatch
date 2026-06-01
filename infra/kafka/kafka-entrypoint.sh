#!/bin/bash
# =============================================================================
# kafka-entrypoint.sh
# Generates /etc/kafka/kafka_server_jaas.conf and /etc/kafka/client.properties
# with actual password values from environment variables, then hands off to
# the standard Kafka entrypoint.
#
# Kafka's JAAS parser does NOT support ${VAR} substitution — passwords must
# be literal values in the file. This script resolves that at container start.
# =============================================================================
set -e

cat > /etc/kafka/kafka_server_jaas.conf <<EOF
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="${KAFKA_ADMIN_PASSWORD}"
    user_admin="${KAFKA_ADMIN_PASSWORD}"
    user_ingest-service="${KAFKA_INGEST_PASSWORD}"
    user_dispatch-service="${KAFKA_DISPATCH_PASSWORD}"
    user_notification-service="${KAFKA_NOTIFICATION_PASSWORD}"
    user_gateway-service="${KAFKA_GATEWAY_PASSWORD}"
    user_tracking-service="${KAFKA_TRACKING_PASSWORD}";
};

KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="${KAFKA_ADMIN_PASSWORD}";
};
EOF

cat > /etc/kafka/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

echo "[kafka-entrypoint] Generated JAAS config and client.properties with live credentials."

# Hand off to the standard Confluent Kafka entrypoint
exec /etc/confluent/docker/run
