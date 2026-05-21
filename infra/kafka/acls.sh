#!/bin/bash
# =============================================================================
# Kafka ACL Definitions — Principle of Least Privilege (ADR 004)
# =============================================================================
# Each service is restricted to only the topics it needs to produce or consume.
# Run this script after kafka-init creates the topics.
#
# ACL matrix:
#   ingest-service:       PRODUCE → gps-pings
#   dispatch-service:     PRODUCE → ride-events
#                         CONSUME → ride-events (dispatch-consumer-group)
#                         CONSUME → gps-pings   (dispatch-location-group)
#   notification-service: CONSUME → ride-events (notification-service-group)
#   gateway-service:      CONSUME → ride-events (gateway-consumer-group)
#   tracking-service:     CONSUME → gps-pings   (tracking-consumer-group)
#
# Requirements: 10.4, ADR 004
# =============================================================================

set -e

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CLIENT_CONFIG="/etc/kafka/client.properties"

echo "Setting Kafka ACLs..."

# ── ingest-service: PRODUCE to gps-pings ─────────────────────────────────────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:ingest-service" \
  --operation Write \
  --topic gps-pings

# ── dispatch-service: PRODUCE to ride-events ─────────────────────────────────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:dispatch-service" \
  --operation Write \
  --topic ride-events

# ── dispatch-service: CONSUME from ride-events (dispatch-consumer-group) ─────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:dispatch-service" \
  --operation Read \
  --topic ride-events

kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:dispatch-service" \
  --operation Read \
  --group dispatch-consumer-group

# ── dispatch-service: CONSUME from gps-pings (dispatch-location-group) ───────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:dispatch-service" \
  --operation Read \
  --topic gps-pings

kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:dispatch-service" \
  --operation Read \
  --group dispatch-location-group

# ── notification-service: CONSUME from ride-events ───────────────────────────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:notification-service" \
  --operation Read \
  --topic ride-events

kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:notification-service" \
  --operation Read \
  --group notification-service-group

# ── gateway-service: CONSUME from ride-events ────────────────────────────────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:gateway-service" \
  --operation Read \
  --topic ride-events

kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:gateway-service" \
  --operation Read \
  --group gateway-consumer-group

# ── tracking-service: CONSUME from gps-pings ─────────────────────────────────
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:tracking-service" \
  --operation Read \
  --topic gps-pings

kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --add \
  --allow-principal "User:tracking-service" \
  --operation Read \
  --group tracking-consumer-group

echo "All Kafka ACLs set successfully."
kafka-acls --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CLIENT_CONFIG" \
  --list
