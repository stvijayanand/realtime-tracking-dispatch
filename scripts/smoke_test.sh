#!/usr/bin/env bash
# =============================================================================
# smoke_test.sh — End-to-end pipeline validation
#
# Validates the full GPS-ping-to-notification pipeline:
#   Driver Simulator → Ingest Service → Kafka → Dispatch Service
#   → Kafka → Notification Service (TripAssigned log line)
#
# Requirements: 11.1, 11.2, 11.4, 11.5
#
# Usage:
#   chmod +x scripts/smoke_test.sh
#   ./scripts/smoke_test.sh
#
# Prerequisites:
#   - docker-compose stack is running (docker-compose up -d)
#   - python3 is available on PATH
#   - curl is available on PATH
#   - jq is available on PATH (optional — falls back to grep/sed)
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[PASS]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[FAIL]${RESET}  $*"; }
step()    { echo -e "\n${BOLD}━━━ $* ━━━${RESET}"; }

# ── Configuration ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

INGEST_URL="${INGEST_URL:-http://localhost:8001}"
DISPATCH_URL="${DISPATCH_URL:-http://localhost:8080}"
NOTIFICATION_CONTAINER="${NOTIFICATION_CONTAINER:-notification-service}"
INGEST_CONTAINER="${INGEST_CONTAINER:-ingest-service}"
DISPATCH_CONTAINER="${DISPATCH_CONTAINER:-dispatch-service}"

DRIVER_ID="smoke-driver-001"
RIDER_ID="smoke-rider-001"
ROUTE_FILE="${SCRIPT_DIR}/sample_route.geojson"
SIMULATOR_SCRIPT="${SCRIPT_DIR}/simulate_driver.py"

SIMULATOR_DURATION=5      # seconds to run the driver simulator
POLL_INTERVAL=1           # seconds between notification log polls
POLL_TIMEOUT=10           # maximum seconds to wait for TripAssigned log line
DIAGNOSTIC_LOG_LINES=20   # lines of logs to print per service on timeout

# ── Utility: JSON field extraction ─────────────────────────────────────────────
# Uses jq if available; falls back to grep + sed for minimal dependencies.
extract_json_field() {
    local json="$1"
    local field="$2"

    if command -v jq &>/dev/null; then
        echo "${json}" | jq -r ".${field} // empty"
    else
        # Fallback: extract "field":"value" or "field": "value"
        echo "${json}" \
            | grep -o "\"${field}\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" \
            | sed 's/.*:[[:space:]]*"\(.*\)"/\1/' \
            | head -1
    fi
}

# ── Utility: check a string is non-empty ───────────────────────────────────────
require_nonempty() {
    local value="$1"
    local label="$2"
    if [[ -z "${value}" ]]; then
        error "${label} is empty — cannot continue."
        return 1
    fi
}

# ── Cleanup trap ───────────────────────────────────────────────────────────────
SIMULATOR_PID=""
cleanup() {
    if [[ -n "${SIMULATOR_PID}" ]] && kill -0 "${SIMULATOR_PID}" 2>/dev/null; then
        info "Stopping Driver Simulator (PID ${SIMULATOR_PID})…"
        kill "${SIMULATOR_PID}" 2>/dev/null || true
        wait "${SIMULATOR_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# =============================================================================
# STEP 1 — Start Driver Simulator for 5 seconds
# =============================================================================
step "Step 1: Start Driver Simulator"

if [[ ! -f "${SIMULATOR_SCRIPT}" ]]; then
    error "Driver Simulator not found at: ${SIMULATOR_SCRIPT}"
    exit 1
fi

if [[ ! -f "${ROUTE_FILE}" ]]; then
    error "Sample route file not found at: ${ROUTE_FILE}"
    exit 1
fi

info "Starting Driver Simulator: driver_id=${DRIVER_ID}, rate=10 pings/sec"
info "Ingest URL: ${INGEST_URL}"

python3 "${SIMULATOR_SCRIPT}" \
    --driver-id "${DRIVER_ID}" \
    --route-file "${ROUTE_FILE}" \
    --rate 10 \
    --ingest-url "${INGEST_URL}" \
    &>/dev/null &

SIMULATOR_PID=$!
info "Driver Simulator started (PID ${SIMULATOR_PID})"

info "Running simulator for ${SIMULATOR_DURATION} seconds…"
sleep "${SIMULATOR_DURATION}"

info "Stopping Driver Simulator (PID ${SIMULATOR_PID})…"
kill "${SIMULATOR_PID}" 2>/dev/null || true
wait "${SIMULATOR_PID}" 2>/dev/null || true
SIMULATOR_PID=""  # prevent double-kill in cleanup trap

success "Driver Simulator ran for ${SIMULATOR_DURATION} seconds and was stopped."

# =============================================================================
# STEP 2 — Submit one Ride Request and capture trip_id
# =============================================================================
step "Step 2: Submit Ride Request"

info "POSTing ride request to ${DISPATCH_URL}/request-ride"
info "  rider_id: ${RIDER_ID}"
info "  pickup:   latitude=37.7749, longitude=-122.4194 (San Francisco)"

RIDE_RESPONSE=$(curl -s \
    --max-time 10 \
    -X POST "${DISPATCH_URL}/request-ride" \
    -H 'Content-Type: application/json' \
    -d "{\"rider_id\":\"${RIDER_ID}\",\"pickup_location\":{\"latitude\":37.7749,\"longitude\":-122.4194}}" \
    2>&1) || {
    error "curl failed to reach Dispatch Service at ${DISPATCH_URL}/request-ride"
    error "Response: ${RIDE_RESPONSE}"
    exit 1
}

info "Dispatch Service response: ${RIDE_RESPONSE}"

# Extract trip_id — the Dispatch Service returns {"trip_id": "<uuid>"}
TRIP_ID=$(extract_json_field "${RIDE_RESPONSE}" "trip_id")

if [[ -z "${TRIP_ID}" ]]; then
    error "Could not extract trip_id from Dispatch Service response."
    error "Raw response: ${RIDE_RESPONSE}"
    error "Expected JSON body with a 'trip_id' field (HTTP 202)."
    exit 1
fi

success "Ride request accepted. trip_id=${TRIP_ID}"

# =============================================================================
# STEP 3 — Poll Notification Service logs for TripAssigned event
# =============================================================================
step "Step 3: Poll Notification Service logs for TripAssigned event"

info "Polling 'docker logs ${NOTIFICATION_CONTAINER}' every ${POLL_INTERVAL}s"
info "Looking for: trip_id=${TRIP_ID} AND event_type=TripAssigned"
info "Timeout: ${POLL_TIMEOUT} seconds"

ELAPSED=0
FOUND=false

while [[ ${ELAPSED} -lt ${POLL_TIMEOUT} ]]; do
    # Capture all notification-service logs since container start
    NOTIFICATION_LOGS=$(docker logs "${NOTIFICATION_CONTAINER}" 2>&1) || {
        warn "Could not read logs from container '${NOTIFICATION_CONTAINER}' (attempt ${ELAPSED}s)"
        sleep "${POLL_INTERVAL}"
        ELAPSED=$((ELAPSED + POLL_INTERVAL))
        continue
    }

    # Search for a log line containing both the trip_id and TripAssigned event_type.
    # The Notification Service emits structured JSON; we look for both strings on the same line.
    MATCH=$(echo "${NOTIFICATION_LOGS}" \
        | grep "${TRIP_ID}" \
        | grep '"event_type":"TripAssigned"' \
        || true)

    if [[ -n "${MATCH}" ]]; then
        FOUND=true
        break
    fi

    info "  [${ELAPSED}s/${POLL_TIMEOUT}s] TripAssigned log line not yet found — waiting…"
    sleep "${POLL_INTERVAL}"
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

# =============================================================================
# STEP 4 — Success path
# =============================================================================
if [[ "${FOUND}" == "true" ]]; then
    step "Result: PASS"
    success "TripAssigned log line found in Notification Service within ${ELAPSED}s!"
    success "trip_id=${TRIP_ID}"
    echo ""
    echo -e "${GREEN}${BOLD}✓ Full pipeline validated:${RESET}"
    echo "  Driver Simulator → Ingest Service → Kafka (gps-pings)"
    echo "  → Dispatch Service → Kafka (ride-events, TripAssigned)"
    echo "  → Notification Service (structured JSON log line)"
    echo ""
    echo "Matching log line:"
    echo "${MATCH}"
    exit 0
fi

# =============================================================================
# STEP 5 — Timeout: diagnostic output
# =============================================================================
step "Result: TIMEOUT — Diagnosing pipeline failure"

error "TripAssigned log line for trip_id=${TRIP_ID} was NOT found within ${POLL_TIMEOUT}s."
echo ""
echo "Checking each pipeline stage to identify where the flow broke…"
echo ""

# ── Stage 1: Ingest Service ────────────────────────────────────────────────────
echo -e "${BOLD}── Stage 1: Ingest Service (${INGEST_CONTAINER}) ──${RESET}"
INGEST_LOGS=$(docker logs --tail "${DIAGNOSTIC_LOG_LINES}" "${INGEST_CONTAINER}" 2>&1) || {
    error "Could not read logs from container '${INGEST_CONTAINER}'."
    error "Is the container running? Run: docker ps | grep ${INGEST_CONTAINER}"
    INGEST_LOGS=""
}

if [[ -n "${INGEST_LOGS}" ]]; then
    # Check for LocationPingReceived events from our driver
    # grep -c returns exit code 1 when count is 0; || true prevents set -e from exiting
    INGEST_PING_COUNT=$(docker logs "${INGEST_CONTAINER}" 2>&1 \
        | grep -c "${DRIVER_ID}" || true)

    if [[ "${INGEST_PING_COUNT}" -gt 0 ]]; then
        success "Ingest Service received ${INGEST_PING_COUNT} ping(s) from ${DRIVER_ID} ✓"
    else
        error "Ingest Service shows NO pings from driver '${DRIVER_ID}'."
        error "→ Pipeline broke at Stage 1: GPS pings did not reach the Ingest Service."
        error "  Check: Is the Ingest Service running on ${INGEST_URL}?"
        error "  Check: Is the Driver Simulator pointing to the correct --ingest-url?"
    fi

    echo ""
    echo "Last ${DIAGNOSTIC_LOG_LINES} lines of Ingest Service logs:"
    echo "─────────────────────────────────────────────────────────"
    echo "${INGEST_LOGS}"
    echo "─────────────────────────────────────────────────────────"
else
    error "No logs available from Ingest Service container '${INGEST_CONTAINER}'."
fi

echo ""

# ── Stage 2: Dispatch Service ──────────────────────────────────────────────────
echo -e "${BOLD}── Stage 2: Dispatch Service (${DISPATCH_CONTAINER}) ──${RESET}"
DISPATCH_LOGS=$(docker logs --tail "${DIAGNOSTIC_LOG_LINES}" "${DISPATCH_CONTAINER}" 2>&1) || {
    error "Could not read logs from container '${DISPATCH_CONTAINER}'."
    error "Is the container running? Run: docker ps | grep ${DISPATCH_CONTAINER}"
    DISPATCH_LOGS=""
}

if [[ -n "${DISPATCH_LOGS}" ]]; then
    # Check for our trip_id in Dispatch logs
    DISPATCH_TRIP_MATCH=$(docker logs "${DISPATCH_CONTAINER}" 2>&1 \
        | grep "${TRIP_ID}" || true)

    if [[ -n "${DISPATCH_TRIP_MATCH}" ]]; then
        success "Dispatch Service processed trip_id=${TRIP_ID} ✓"

        # Check if TripAssigned was published
        TRIP_ASSIGNED_MATCH=$(docker logs "${DISPATCH_CONTAINER}" 2>&1 \
            | grep "${TRIP_ID}" \
            | grep -i "TripAssigned\|assigned" || true)

        if [[ -n "${TRIP_ASSIGNED_MATCH}" ]]; then
            success "Dispatch Service published TripAssigned for trip_id=${TRIP_ID} ✓"
            error "→ Pipeline broke at Stage 3: TripAssigned event was published but"
            error "  Notification Service did not log it within ${POLL_TIMEOUT}s."
            error "  Check: Is the Notification Service consuming from 'ride-events' topic?"
            error "  Check: Is the Notification Service in the correct consumer group?"
        else
            error "Dispatch Service received the ride request but did NOT publish TripAssigned."
            error "→ Pipeline broke at Stage 2: Dispatch Service failed to assign a driver."
            error "  Check: Is Kafka healthy? (docker logs kafka-1)"
            error "  Check: Is the 'ride-events' topic created? (docker exec kafka-1 kafka-topics.sh --list ...)"
        fi
    else
        error "Dispatch Service has NO record of trip_id=${TRIP_ID}."
        error "→ Pipeline broke at Stage 2: Ride request did not reach Dispatch Service"
        error "  or was rejected. Check the curl response above."
        error "  Check: Is the Dispatch Service running on ${DISPATCH_URL}?"
    fi

    echo ""
    echo "Last ${DIAGNOSTIC_LOG_LINES} lines of Dispatch Service logs:"
    echo "─────────────────────────────────────────────────────────"
    echo "${DISPATCH_LOGS}"
    echo "─────────────────────────────────────────────────────────"
else
    error "No logs available from Dispatch Service container '${DISPATCH_CONTAINER}'."
fi

echo ""

# ── Stage 3: Notification Service ─────────────────────────────────────────────
echo -e "${BOLD}── Stage 3: Notification Service (${NOTIFICATION_CONTAINER}) ──${RESET}"
NOTIFICATION_LOGS_TAIL=$(docker logs --tail "${DIAGNOSTIC_LOG_LINES}" "${NOTIFICATION_CONTAINER}" 2>&1) || {
    error "Could not read logs from container '${NOTIFICATION_CONTAINER}'."
    error "Is the container running? Run: docker ps | grep ${NOTIFICATION_CONTAINER}"
    NOTIFICATION_LOGS_TAIL=""
}

if [[ -n "${NOTIFICATION_LOGS_TAIL}" ]]; then
    # Check if Notification Service is consuming any events at all
    # grep -c returns exit code 1 when count is 0; || true prevents set -e from exiting
    NOTIFICATION_ANY_TRIP=$(docker logs "${NOTIFICATION_CONTAINER}" 2>&1 \
        | grep -c "TripAssigned" || true)

    if [[ "${NOTIFICATION_ANY_TRIP}" -gt 0 ]]; then
        warn "Notification Service has logged ${NOTIFICATION_ANY_TRIP} TripAssigned event(s),"
        warn "but none matched trip_id=${TRIP_ID}."
        warn "→ The Notification Service is working, but this specific trip was not matched."
        warn "  This may indicate a timing issue — try increasing POLL_TIMEOUT."
    else
        error "Notification Service has NOT logged any TripAssigned events."
        error "→ The Notification Service is not receiving TripAssigned events from Kafka."
        error "  Check: Is the Notification Service connected to Kafka?"
        error "  Check: Is the consumer group 'notification-service-group' active?"
        error "  Run: docker exec kafka-1 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-service-group"
    fi

    echo ""
    echo "Last ${DIAGNOSTIC_LOG_LINES} lines of Notification Service logs:"
    echo "─────────────────────────────────────────────────────────"
    echo "${NOTIFICATION_LOGS_TAIL}"
    echo "─────────────────────────────────────────────────────────"
else
    error "No logs available from Notification Service container '${NOTIFICATION_CONTAINER}'."
fi

echo ""
echo -e "${RED}${BOLD}✗ Smoke test FAILED.${RESET}"
echo "  trip_id=${TRIP_ID} did not produce a TripAssigned log line within ${POLL_TIMEOUT}s."
echo ""
echo "Suggested next steps:"
echo "  1. Run 'docker-compose ps' to verify all containers are healthy."
echo "  2. Run 'docker-compose logs -f' to tail all service logs."
echo "  3. Check Kafka topic offsets: docker exec kafka-1 kafka-consumer-groups.sh \\"
echo "       --bootstrap-server localhost:9092 --describe --all-groups"
echo "  4. Check Jaeger traces at http://localhost:16686 for the distributed trace."
echo ""

exit 1
