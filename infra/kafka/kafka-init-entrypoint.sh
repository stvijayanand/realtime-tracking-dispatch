#!/bin/bash
# Generates client.properties with the live admin password, then creates topics.
set -e

cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

echo "[kafka-init] Generated client.properties."
export KAFKA_CLIENT_PROPERTIES=/tmp/client.properties
exec bash /create-topics.sh
