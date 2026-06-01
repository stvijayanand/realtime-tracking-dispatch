#!/bin/bash
# Creates all required Kafka topics on first startup.
# Requirement 7.3: replication.factor=3, min.insync.replicas=2
set -e

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CLIENT_CONFIG="${KAFKA_CLIENT_PROPERTIES:-/etc/kafka/client.properties}"

echo "Waiting for Kafka brokers to be ready..."
sleep 10

create_topic() {
  local topic=$1
  local partitions=${2:-3}
  echo "Creating topic: $topic"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --command-config "$CLIENT_CONFIG" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=604800000
}

create_topic "gps-pings"
create_topic "ride-events"
create_topic "dispatch-commands"
create_topic "notifications"

echo "All topics created successfully."
kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --list
