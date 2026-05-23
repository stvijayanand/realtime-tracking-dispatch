#!/usr/bin/env bash
# =============================================================================
# throughput_test.sh — Sustained throughput validation for the Ingest Service
#
# Runs the Driver Simulator at 10 pings/second for 5 seconds (50 total pings)
# and asserts zero HTTP 5xx responses from the Ingest Service during the run.
#
# Requirements: 11.3
# =============================================================================
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
INGEST_URL="${INGEST_URL:-http://localhost:8001}"
ROUTE_FILE="${ROUTE_FILE:-scripts/sample_route.geojson}"
DRIVER_ID="throughput-driver-001"
RATE=10
DURATION_SECONDS=5
EXPECTED_PINGS=$(( RATE * DURATION_SECONDS ))   # 50

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIMULATOR="${SCRIPT_DIR}/simulate_driver.py"
STDERR_LOG="$(mktemp /tmp/throughput_test_stderr.XXXXXX)"

# Resolve route file relative to repo root if not absolute
if [[ "${ROUTE_FILE}" != /* ]]; then
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    ROUTE_FILE="${REPO_ROOT}/${ROUTE_FILE}"
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
print_header() {
    echo ""
    echo "============================================================"
    echo "  Throughput Test — Ingest Service"
    echo "  Rate: ${RATE} pings/sec × ${DURATION_SECONDS}s = ${EXPECTED_PINGS} total pings"
    echo "  Driver ID: ${DRIVER_ID}"
    echo "  Ingest URL: ${INGEST_URL}"
    echo "  Route file: ${ROUTE_FILE}"
    echo "============================================================"
    echo ""
}

cleanup() {
    # Kill the simulator if still running
    if [[ -n "${SIM_PID:-}" ]] && kill -0 "${SIM_PID}" 2>/dev/null; then
        kill "${SIM_PID}" 2>/dev/null || true
        wait "${SIM_PID}" 2>/dev/null || true
    fi
    rm -f "${STDERR_LOG}"
}
trap cleanup EXIT

# ── Pre-flight checks ─────────────────────────────────────────────────────────
echo "[INFO] Running pre-flight checks..."

if ! command -v python3 &>/dev/null; then
    echo "[ERROR] python3 is not installed or not on PATH" >&2
    exit 1
fi

if [[ ! -f "${SIMULATOR}" ]]; then
    echo "[ERROR] Driver Simulator not found at: ${SIMULATOR}" >&2
    exit 1
fi

if [[ ! -f "${ROUTE_FILE}" ]]; then
    echo "[ERROR] Route file not found at: ${ROUTE_FILE}" >&2
    exit 1
fi

# Check Ingest Service health before starting
echo "[INFO] Checking Ingest Service health at ${INGEST_URL}/health ..."
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${INGEST_URL}/health" 2>/dev/null || echo "000")
if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "[ERROR] Ingest Service health check failed (HTTP ${HTTP_STATUS})." >&2
    echo "[ERROR] Ensure the Ingest Service is running at ${INGEST_URL} before running this test." >&2
    exit 1
fi
echo "[INFO] Ingest Service is healthy (HTTP 200)."

# ── Run the Driver Simulator ──────────────────────────────────────────────────
print_header

echo "[INFO] Starting Driver Simulator (rate=${RATE} pings/sec, duration=${DURATION_SECONDS}s)..."
echo "[INFO] Simulator stderr will be captured to: ${STDERR_LOG}"
echo ""

# Launch simulator in background, capturing stderr to file
python3 "${SIMULATOR}" \
    --driver-id "${DRIVER_ID}" \
    --route-file "${ROUTE_FILE}" \
    --rate "${RATE}" \
    --ingest-url "${INGEST_URL}" \
    2>"${STDERR_LOG}" &
SIM_PID=$!

echo "[INFO] Simulator PID: ${SIM_PID}"
echo "[INFO] Running for ${DURATION_SECONDS} seconds..."

# Wait for the configured duration, then stop the simulator
sleep "${DURATION_SECONDS}"

echo ""
echo "[INFO] ${DURATION_SECONDS}s elapsed — stopping simulator (PID ${SIM_PID})..."
kill "${SIM_PID}" 2>/dev/null || true
wait "${SIM_PID}" 2>/dev/null || true
SIM_PID=""   # Mark as stopped so cleanup() doesn't try again

echo "[INFO] Simulator stopped."
echo ""

# ── Parse simulator stderr for 5xx errors ────────────────────────────────────
echo "[INFO] Parsing simulator stderr for HTTP 5xx responses..."

# Match lines containing "HTTP 5" followed by two digits (e.g. "HTTP 500", "HTTP 503")
SIMULATOR_5XX_LINES=$(grep -E 'HTTP 5[0-9]{2}' "${STDERR_LOG}" || true)
SIMULATOR_5XX_COUNT=$(echo "${SIMULATOR_5XX_LINES}" | grep -c 'HTTP 5[0-9]\{2\}' || true)

# ── Check docker logs for Ingest Service 5xx errors ──────────────────────────
echo "[INFO] Checking docker logs for ingest-service ERROR lines..."

DOCKER_5XX_LINES=""
DOCKER_5XX_COUNT=0

if command -v docker &>/dev/null; then
    # Capture recent ingest-service logs (last 200 lines to cover the test window)
    DOCKER_LOG_OUTPUT=$(docker logs ingest-service --tail 200 2>&1 || true)
    # Look for ERROR lines that mention 5xx status codes
    DOCKER_5XX_LINES=$(echo "${DOCKER_LOG_OUTPUT}" | grep -E '(ERROR|error).*5[0-9]{2}|5[0-9]{2}.*(ERROR|error)' || true)
    DOCKER_5XX_COUNT=$(echo "${DOCKER_5XX_LINES}" | grep -c '.' || true)
else
    echo "[WARN] docker command not found — skipping docker log check."
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Throughput Test Summary"
echo "============================================================"
echo "  Expected pings sent : ${EXPECTED_PINGS}"
echo "  Test duration       : ${DURATION_SECONDS}s"
echo "  Rate                : ${RATE} pings/sec"
echo ""

TOTAL_ERRORS=$(( SIMULATOR_5XX_COUNT + DOCKER_5XX_COUNT ))

if [[ "${SIMULATOR_5XX_COUNT}" -gt 0 ]]; then
    echo "  [FAIL] Simulator detected ${SIMULATOR_5XX_COUNT} HTTP 5xx response(s):"
    echo ""
    echo "${SIMULATOR_5XX_LINES}" | while IFS= read -r line; do
        echo "    ${line}"
    done
    echo ""
else
    echo "  [PASS] Simulator: 0 HTTP 5xx responses detected."
fi

if [[ "${DOCKER_5XX_COUNT}" -gt 0 ]]; then
    echo "  [FAIL] Docker logs show ${DOCKER_5XX_COUNT} ERROR line(s) related to 5xx:"
    echo ""
    echo "${DOCKER_5XX_LINES}" | while IFS= read -r line; do
        echo "    ${line}"
    done
    echo ""
else
    echo "  [PASS] Docker logs: 0 5xx-related ERROR lines in ingest-service."
fi

echo ""
echo "============================================================"

if [[ "${TOTAL_ERRORS}" -gt 0 ]]; then
    echo "  RESULT: FAILED — ${TOTAL_ERRORS} 5xx error(s) observed during sustained throughput run."
    echo "============================================================"
    echo ""
    exit 1
else
    echo "  RESULT: PASSED — Zero HTTP 5xx errors at ${RATE} pings/sec for ${DURATION_SECONDS}s."
    echo "============================================================"
    echo ""
    exit 0
fi
