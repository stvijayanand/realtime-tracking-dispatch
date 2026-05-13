# Implementation Plan: e2e-skeleton

## Overview

Build the foundational end-to-end data flow for the Real-Time Ride/Delivery Tracking & Dispatch Platform. The implementation proceeds infrastructure-first: monorepo scaffold and shared schema, then each service with its property-based tests, then the docker-compose environment with security controls, and finally the smoke test and end-to-end validation. Each step is independently verifiable before the next begins.

Languages: Python 3.11 (Ingest, Notification, Driver Simulator), Java 21 / Spring Boot 3.x (Dispatch), TypeScript / React 18 (Rider UI), Bash (scripts).

---

## Tasks

- [ ] 1. Monorepo directory scaffold and shared Kafka envelope schema
  - [ ] 1.1 Create the top-level directory structure
    - Create `services/ingest/`, `services/dispatch/`, `services/notification/`, `services/tracking/`, `services/gateway/`
    - Create `infra/docker/`, `infra/k8s/`, `infra/kafka/`
    - Create `scripts/`
    - Create `shared/` with a `README.md` stating it is restricted to infrastructure concerns only
    - Create placeholder `docker-compose.yml` at the repository root (to be filled in Task 8)
    - Create `.env.example` at the repository root with placeholder entries for every environment variable referenced in the design (Kafka bootstrap, SASL credentials per service, PostgreSQL credentials, Redis password, service ports); add `.env` to `.gitignore`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.7, 10.1, 10.2_

  - [ ] 1.2 Define the shared Kafka Domain Event envelope schema
    - Create `shared/kafka_envelope.py` (Python dataclass / TypedDict) and `shared/KafkaEnvelope.java` (Java record) defining the standard envelope: `event_id` (UUID string), `event_type` (string), `occurred_at` (ISO 8601 string), `payload` (object)
    - Create `shared/envelope_schema.json` as a JSON Schema document describing the envelope for cross-language reference
    - Document the envelope fields and the `event_id` immutability rule in `shared/README.md`
    - _Requirements: 1.4, 2.2, 3.3, 3.4_

- [ ] 2. FastAPI Ingest Service
  - [ ] 2.1 Scaffold the Ingest Service project structure
    - Create `services/ingest/` with `main.py`, `requirements.txt` (pinned versions: `fastapi`, `uvicorn`, `confluent-kafka`, `pydantic`, `hypothesis`), and `Dockerfile`
    - `Dockerfile` MUST use `python:3.11.9-slim` as the base image, create a non-root user `appuser`, and run the process as that user
    - Add a `GET /health` endpoint returning `{"status": "ok"}`
    - Read all configuration from environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_GPS_PINGS`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `SERVICE_PORT`; exit with a non-zero status code and a descriptive error if any required variable is missing
    - _Requirements: 2.8, 2.11, 10.6, 10.7, 10.10_

  - [ ] 2.2 Implement the `POST /location` endpoint with input validation
    - Define a Pydantic request model with `driver_id` (non-empty string, max 128 chars), `latitude` (float, −90 to 90), `longitude` (float, −180 to 180), `timestamp` (ISO 8601 string)
    - Return HTTP 422 with a structured error body for missing fields, out-of-range coordinates, empty `driver_id`, or `driver_id` exceeding 128 characters
    - Enforce a 64 KB maximum request body size; return HTTP 413 if exceeded
    - _Requirements: 2.1, 2.4, 2.5, 2.10, 10.8, 10.9_

  - [ ] 2.3 Implement the Kafka producer and `LocationPingReceived` event publishing
    - Configure the `confluent-kafka` producer with `enable.idempotence=true` and SASL/PLAIN credentials from environment variables
    - On valid request: generate a UUID `event_id`, build the `LocationPingReceived` envelope, publish to `gps-pings` with `driver_id` as the message key
    - Return HTTP 202 `{"message_id": "<event_id>"}` on successful publish
    - Return HTTP 503 (do not silently drop) if the topic is unavailable; log a warning with the broker address
    - _Requirements: 2.2, 2.3, 2.6, 2.9_

  - [ ]* 2.4 Write property tests for the Ingest Service (Hypothesis)
    - **Property 1: GPS ping event envelope round-trip** — generate random valid `driver_id` (1–128 chars), `latitude` in [−90, 90], `longitude` in [−180, 180], ISO 8601 `timestamp`; assert HTTP 202 and that `message_id` equals the `event_id` in the captured Kafka message, with correct `event_type` and preserved payload fields. Mock the Kafka producer.
    - **Property 2: Ingest Service rejects invalid GPS ping inputs** — generate requests with at least one invalid condition (missing field, out-of-range coordinate, empty `driver_id`, `driver_id` > 128 chars); assert HTTP 422 and that the Kafka producer is never called.
    - **Property 5 (Ingest side): HTTP request body size enforcement** — generate payloads whose byte size straddles the 64 KB boundary; assert HTTP 413 for oversized payloads and normal processing for valid-sized payloads.
    - Tag each test with `# Feature: e2e-skeleton, Property N: <title>`; run minimum 100 iterations per property
    - _Requirements: 2.1, 2.2, 2.4, 2.5, 2.6, 2.10_

  - [ ] 2.5 Generate and commit the Ingest Service OpenAPI spec
    - Configure FastAPI to auto-generate the OpenAPI spec at application startup
    - Add a startup hook that writes the spec to `services/ingest/openapi.json`
    - Verify the spec includes the `POST /location` request schema, all response codes (202, 413, 422, 503), and the `GET /health` endpoint
    - _Requirements: 2.7, 8.1, 8.4, 8.6_

- [ ] 3. Spring Boot Dispatch Service
  - [ ] 3.1 Scaffold the Dispatch Service project structure
    - Create `services/dispatch/` with a Maven `pom.xml` (Spring Boot 3.x, Spring Kafka, Spring Data JPA, springdoc-openapi, jqwik, H2 for tests, PostgreSQL driver); pinned dependency versions
    - `Dockerfile` MUST use `eclipse-temurin:21-jre-jammy` as the base image, create a non-root user `appuser`, and run the process as that user
    - Read all configuration from environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_RIDE_EVENTS`, `KAFKA_TOPIC_GPS_PINGS`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_CONSUMER_GROUP_RIDE_EVENTS`, `KAFKA_CONSUMER_GROUP_GPS_PINGS`, `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, `SERVICE_PORT`; fail fast with a descriptive error if any required variable is missing
    - Implement exponential backoff retry on Kafka broker connection at startup (up to 5 attempts, then exit non-zero)
    - _Requirements: 3.5, 3.6, 3.12, 10.6, 10.7, 10.10_

  - [ ] 3.2 Define the PostgreSQL `trips` table and Trip entity
    - Create the `trips` table schema (Flyway or Liquibase migration): `trip_id` (UUID PK), `rider_id` (VARCHAR 128), `driver_id` (VARCHAR 128, nullable), `status` (VARCHAR 20: `REQUESTED`, `ASSIGNED`, `CANCELLED`), `pickup_lat`, `pickup_lng`, `requested_at`, `assigned_at` (nullable), `updated_at`
    - Create indexes `idx_trips_status` and `idx_trips_updated_at`
    - Implement the `Trip` JPA entity and `TripRepository` (Spring Data JPA)
    - Model `TripCancelled` as a domain event class in code (event type, envelope fields) even though it is not triggered in Phase 1
    - _Requirements: 3.16, 3.17_

  - [ ] 3.3 Implement the `POST /request-ride` HTTP endpoint
    - Accept JSON body with `rider_id` (non-empty string, max 128 chars) and `pickup_location` (`latitude`, `longitude`); return HTTP 422 for invalid input
    - Enforce 64 KB maximum request body size; return HTTP 413 if exceeded
    - Generate a `trip_id` UUID, persist a `Trip` record with `status = REQUESTED`, publish a `TripRequested` Domain Event to `ride-events` (idempotent producer, `enable.idempotence=true`), return HTTP 202 `{"trip_id": "<uuid>"}`
    - Expose `GET /health` returning `{"status": "UP"}` and `GET /v3/api-docs` via springdoc-openapi
    - _Requirements: 3.4, 3.8, 3.10, 3.11, 10.8, 10.9_

  - [ ] 3.4 Implement the `ride-events` Kafka consumer (dispatch-consumer-group)
    - Consume from `ride-events` topic using consumer group `dispatch-consumer-group`
    - Filter for `event_type == "TripRequested"`; acknowledge and skip all other event types
    - On `TripRequested`: select a driver from a static in-memory list (hardcoded Phase 1 matching), update the `Trip` record to `status = ASSIGNED` with `driver_id` and `assigned_at`, publish a `TripAssigned` Domain Event to `ride-events`
    - Complete dispatch and publish within 2 seconds of consumption; log a WARNING if exceeded
    - _Requirements: 3.1, 3.2, 3.3, 3.7, 3.16_

  - [ ] 3.5 Implement the `gps-pings` Kafka consumer (dispatch-location-group)
    - Consume from `gps-pings` topic using consumer group `dispatch-location-group` (separate from any other consumer group)
    - Validate envelope: assert `event_type == "LocationPingReceived"` and `event_id` is a non-empty UUID; log at DEBUG level on success
    - On deserialization failure or envelope validation failure: log WARNING to stderr, commit offset, continue without crashing
    - Do NOT write to Redis in Phase 1 (CQRS read model stub per ADR 005)
    - _Requirements: 3.13, 3.14, 3.15_

  - [ ]* 3.6 Write property tests for the Dispatch Service (jqwik)
    - **Property 3: TripAssigned envelope correctness** — generate random `trip_id` UUIDs, `rider_id` strings (1–128 chars), valid `pickup_location` coordinates; assert the resulting `TripAssigned` event has a non-empty `event_id` UUID, `event_type = "TripAssigned"`, valid ISO 8601 `occurred_at`, same `trip_id`, non-empty `driver_id` from the static list, same `rider_id`, valid ISO 8601 `assigned_at`. Mock Kafka producer and PostgreSQL.
    - **Property 4: Dispatch event type filtering** — generate random `event_type` strings excluding `"TripRequested"`; assert driver assignment logic is never triggered and no `TripAssigned` event is published.
    - **Property 5 (Dispatch side): HTTP request body size enforcement** — generate payloads straddling the 64 KB boundary for `POST /request-ride`; assert HTTP 413 for oversized payloads.
    - **Property 6: gps-pings envelope validation** — generate valid envelopes, envelopes with missing `event_id`, wrong `event_type`, and non-JSON bytes; assert WARNING logged and consumer continues without crashing for invalid inputs; assert DEBUG log for valid inputs.
    - **Property 9: Trip state machine persistence round-trip** — generate random `rider_id` and `pickup_location`; assert `trips` table contains `status = REQUESTED` after `POST /request-ride` returns 202, then `status = ASSIGNED` with non-null `driver_id` and `assigned_at` after `TripAssigned` is published. Use H2 in-memory DB.
    - **Property 12: Startup fails fast on missing environment variables** — omit each required env var in turn; assert non-zero exit and descriptive error log identifying the missing variable.
    - Tag each test with `// Feature: e2e-skeleton, Property N: <title>`; run minimum 100 iterations per property
    - _Requirements: 3.1, 3.3, 3.11, 3.13, 3.14, 3.15, 3.16, 10.10_

  - [ ] 3.7 Generate and commit the Dispatch Service OpenAPI spec
    - Verify springdoc-openapi exposes `/v3/api-docs` with the `POST /request-ride` endpoint, request schema, and all response codes (202, 413, 422)
    - Add a script or Maven goal that fetches `/v3/api-docs` and writes the output to `services/dispatch/openapi.json`
    - _Requirements: 3.8, 3.9, 8.3, 8.4, 8.6_

- [ ] 4. Checkpoint — Ingest and Dispatch unit tests pass
  - Ensure all unit and property tests for the Ingest Service and Dispatch Service pass. Ask the user if questions arise.

- [ ] 5. FastAPI Notification Service
  - [ ] 5.1 Scaffold the Notification Service project structure
    - Create `services/notification/` with `main.py`, `requirements.txt` (pinned: `fastapi`, `uvicorn`, `confluent-kafka`, `pydantic`, `hypothesis`), and `Dockerfile`
    - `Dockerfile` MUST use `python:3.11.9-slim`, create non-root user `appuser`, run as that user
    - Expose `GET /health` returning `{"status": "ok"}`
    - Read all configuration from environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_RIDE_EVENTS`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_CONSUMER_GROUP_ID`, `SERVICE_PORT`; exit non-zero with a descriptive error if any required variable is missing
    - _Requirements: 4.6, 4.7, 4.9, 10.6, 10.7, 10.10_

  - [ ] 5.2 Implement the `ride-events` Kafka consumer and notification logger
    - Consume from `ride-events` using consumer group from `KAFKA_CONSUMER_GROUP_ID` (distinct from `dispatch-consumer-group`)
    - Filter for `event_type == "TripAssigned"`; acknowledge and skip all other event types without logging an error
    - On `TripAssigned`: write a structured JSON line to stdout containing `event_id`, `event_type`, `trip_id`, `driver_id`, `rider_id`, `assigned_at`, and `notification_sent_at`
    - On non-JSON message: log WARNING to stderr with raw message bytes, commit offset, continue
    - Duplicate `TripAssigned` deliveries: log again (idempotent log writes acceptable in Phase 1)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.8_

  - [ ]* 5.3 Write property tests for the Notification Service (Hypothesis)
    - **Property 7: Notification Service logs all required fields for TripAssigned events** — generate random `TripAssigned` payloads (any `event_id`, `trip_id`, `driver_id`, `rider_id`, `assigned_at`); assert the stdout JSON line contains all seven required fields. Mock the Kafka consumer.
    - **Property 8: Notification Service filters non-TripAssigned events** — generate random `event_type` strings excluding `"TripAssigned"`; assert no stdout output and no error log is produced.
    - Tag each test with `# Feature: e2e-skeleton, Property N: <title>`; run minimum 100 iterations per property
    - _Requirements: 4.2, 4.3_

  - [ ] 5.4 Generate and commit the Notification Service OpenAPI spec
    - Configure FastAPI to auto-generate the OpenAPI spec at startup and write it to `services/notification/openapi.json`
    - Verify the spec includes the `GET /health` endpoint and all response codes
    - _Requirements: 4.5, 8.2, 8.4, 8.6_

- [ ] 6. Driver Simulator script
  - [ ] 6.1 Implement `scripts/simulate_driver.py`
    - Accept CLI arguments: `--driver-id` (string), `--route-file` (path to GeoJSON LineString), `--rate` (pings/sec, default 10), `--ingest-url` (base URL)
    - Read the GeoJSON LineString from `--route-file`; exit non-zero with an error message if the file is not found or is not a valid GeoJSON LineString
    - Interpolate positions along the route at the configured rate; POST each position to `{ingest-url}/location`
    - On non-2xx response: log error to stderr (status code + response body), continue emitting
    - On route end: loop back to the start (infinite loop until interrupted)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [ ] 6.2 Create `scripts/sample_route.geojson`
    - Write a GeoJSON FeatureCollection containing a LineString with at least 10 coordinate pairs representing a plausible city-scale route
    - _Requirements: 5.7_

  - [ ]* 6.3 Write property tests for the Driver Simulator (Hypothesis)
    - **Property 11: Driver Simulator route looping** — generate valid GeoJSON LineString routes of varying lengths (2–100 coordinate pairs); after the simulator emits a ping for the last coordinate, assert the next emitted ping has coordinates near the first coordinate of the route (within interpolation tolerance). Mock the HTTP POST call.
    - Tag with `# Feature: e2e-skeleton, Property 11: Driver Simulator route looping`; run minimum 100 iterations
    - _Requirements: 5.6_

- [ ] 7. Minimal React Rider UI
  - [ ] 7.1 Scaffold the Rider UI project
    - Create `services/rider-ui/` with a React 18 app (Create React App or Vite); add `react-leaflet` and `leaflet` as pinned dependencies
    - Configure `REACT_APP_DISPATCH_URL` (or `VITE_DISPATCH_URL`) as a build-time environment variable for the Dispatch Service URL
    - _Requirements: 6.1, 6.6_

  - [ ] 7.2 Implement the map view and "Request Ride" button
    - Render a Leaflet map centred on a default coordinate pair at a city-scale zoom level
    - Add a "Request Ride" button that POSTs to `${DISPATCH_URL}/request-ride` with a hardcoded `rider_id` and the map centre as `pickup_location`
    - On HTTP 202: display the returned `trip_id` on screen
    - On non-2xx response: display a human-readable error message; do not leave the user with a blank or crashed page
    - _Requirements: 6.2, 6.3, 6.4, 6.5_

  - [ ] 7.3 Add a production Dockerfile for the Rider UI static build
    - Multi-stage Dockerfile: build stage uses `node:20.12-alpine` (pinned), serve stage uses `nginx:1.25-alpine` (pinned) as a non-root user
    - The built static assets are served by nginx; `REACT_APP_DISPATCH_URL` is injected at build time
    - _Requirements: 6.6, 10.6, 10.7_

- [ ] 8. docker-compose local environment
  - [ ] 8.1 Define infrastructure containers (Redpanda, PostgreSQL, Redis)
    - Add Redpanda container to `docker-compose.yml` with SASL/PLAIN authentication enabled; configure it to create topics `gps-pings`, `ride-events`, `dispatch-commands`, and `notifications` on first startup
    - Add PostgreSQL container with a non-empty password loaded from `${POSTGRES_PASSWORD}`; define a named volume for data persistence
    - Add Redis container with a non-empty password loaded from `${REDIS_PASSWORD}`; define a named volume for data persistence
    - All credentials referenced via `${VAR_NAME}` substitution — no hardcoded values
    - _Requirements: 7.1, 7.3, 7.5, 7.9, 7.10, 10.3, 10.4_

  - [ ] 8.2 Define named Docker networks and assign services
    - Define three named networks: `kafka-net`, `db-net`, `frontend-net`
    - Assign: Redpanda + all services → `kafka-net`; PostgreSQL + Redis + Dispatch Service → `db-net`; Dispatch Service + Rider UI → `frontend-net`
    - Ingest Service and Notification Service MUST NOT be connected to `db-net`; Rider UI MUST NOT be connected to `kafka-net` or `db-net`
    - _Requirements: 7.8, 10.5_

  - [ ] 8.3 Wire all service containers with environment variables and health checks
    - Add service containers for Ingest Service (host port 8001), Dispatch Service (host port 8080), Notification Service (host port 8002), and Rider UI
    - Inject all inter-service hostnames and ports via environment variables; no service hardcodes another service's hostname
    - Configure each service with its own dedicated Kafka SASL credentials (`${INGEST_KAFKA_USERNAME}`, `${INGEST_KAFKA_PASSWORD}`, etc.) — no shared credentials
    - Add health checks for all containers; configure `restart: on-failure` (not `always`) so failures surface in `docker-compose up` output
    - Verify `docker-compose up` reaches a healthy state within 120 seconds
    - _Requirements: 7.2, 7.4, 7.6, 7.7, 7.9, 10.3, 10.4_

- [ ] 9. OpenAPI spec generation and commit script
  - [ ] 9.1 Create the `scripts/generate_openapi.sh` script
    - The script starts each FastAPI service in a temporary subprocess, waits for it to be ready, fetches the generated spec, and writes it to the committed path (`services/ingest/openapi.json`, `services/notification/openapi.json`)
    - For the Dispatch Service, the script starts the Spring Boot app (or uses the running docker-compose container), fetches `/v3/api-docs`, and writes to `services/dispatch/openapi.json`
    - The script exits with a non-zero code if any regenerated spec differs from the committed version (using `diff`), enabling CI enforcement
    - _Requirements: 8.5_

  - [ ]* 9.2 Validate committed OpenAPI specs are valid OpenAPI 3.x
    - Add a check in `generate_openapi.sh` (or a separate `scripts/validate_openapi.sh`) that runs a lightweight OpenAPI validator (e.g., `openapi-spec-validator` Python package) against each committed spec file
    - Exit non-zero if any spec fails validation
    - _Requirements: 8.4_

- [ ] 10. Security baseline hardening
  - [ ] 10.1 Audit and complete `.env.example`
    - Ensure `.env.example` documents every environment variable required by every service and the docker-compose environment, with placeholder values and a comment explaining each variable's purpose
    - Verify `.env` is listed in `.gitignore` and no `.env` file exists in the repository
    - _Requirements: 1.7, 10.1, 10.2_

  - [ ] 10.2 Verify Dockerfile security controls across all services
    - Confirm each Dockerfile (Ingest, Dispatch, Notification, Rider UI) uses a pinned base image version (not `latest`) and includes a `USER appuser` directive
    - Add a `scripts/check_dockerfiles.sh` script that greps each Dockerfile for `FROM.*:latest` and exits non-zero if any match is found
    - _Requirements: 2.11, 3.12, 4.9, 10.6, 10.7_

  - [ ] 10.3 Verify startup fail-fast behaviour for missing environment variables
    - Confirm each service (Ingest, Dispatch, Notification) logs a descriptive error identifying the missing variable and exits non-zero when a required env var is absent
    - Write unit tests for each service's startup configuration validation
    - _Requirements: 10.10_

- [ ] 11. Checkpoint — All service tests pass, docker-compose starts cleanly
  - Ensure all unit and property tests pass for all three services. Run `docker-compose up` and confirm all containers reach a healthy state within 120 seconds. Ask the user if questions arise.

- [ ] 12. Smoke test script and end-to-end validation
  - [ ] 12.1 Implement `scripts/smoke_test.sh`
    - Start the Driver Simulator (`scripts/simulate_driver.py`) for 5 seconds against the running Compose environment
    - Submit one Ride Request via `curl` to `POST /request-ride` and capture the returned `trip_id`
    - Poll the Notification Service container logs (`docker logs`) for a JSON line containing the matching `trip_id` and `event_type = "TripAssigned"`, with a 10-second timeout
    - On match found: exit 0
    - On timeout: exit non-zero and print a diagnostic message identifying which pipeline stage (Ingest, Dispatch, Notification) did not produce the expected output
    - _Requirements: 11.4, 11.5_

  - [ ] 12.2 Validate the end-to-end pipeline invariants in the smoke test
    - Assert that the `trip_id` in the `TripAssigned` log line matches the `trip_id` returned by `POST /request-ride` (Requirement 11.2)
    - Assert that the `event_id` field is present and non-empty in the logged output
    - Assert that the notification appears within 5 seconds of the Ride Request being accepted (Requirement 11.1)
    - _Requirements: 11.1, 11.2_

  - [ ]* 12.3 Write a sustained throughput integration test
    - Write a script or pytest test that sends 10 GPS pings per second to the Ingest Service for 5 seconds (50 total pings) and asserts that no HTTP 5xx responses are returned
    - _Requirements: 11.3_

  - [ ]* 12.4 Write a property test for the trip_id correlation invariant (Hypothesis)
    - **Property 10: trip_id correlation across the full pipeline** — generate random valid ride requests; for each, assert that the `trip_id` returned in the HTTP 202 response from `POST /request-ride` equals the `trip_id` in the `TripAssigned` Domain Event logged by the Notification Service. Run against the docker-compose environment with mocked Kafka or a test Redpanda instance.
    - Tag with `# Feature: e2e-skeleton, Property 10: trip_id correlation across the full pipeline`
    - _Requirements: 11.2_

- [ ] 13. Final checkpoint — Smoke test passes end-to-end
  - Run `scripts/smoke_test.sh` against the full docker-compose environment and confirm it exits 0. Ensure all committed OpenAPI specs are valid and up to date. Ask the user if questions arise.

---

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP; all core pipeline tasks are required
- Each task references specific requirements for traceability
- Property tests use Hypothesis (Python) and jqwik (Java); Kafka interactions are mocked in unit/property tests
- Integration and smoke tests run against the full docker-compose environment
- The Dispatch Service's `gps-pings` consumer is a Phase 1 stub — it logs only and does not write to Redis (CQRS read model deferred to Phase 2 per ADR 005)
- `TripCancelled` is modelled in code from Phase 1 but not triggered until Phase 2 (compensating event per ADR 006)
- The Outbox Pattern and consumer-side deduplication are deferred to Phase 2 (ADR 002, ADR 003)
- All secrets are loaded from environment variables; `.env` is gitignored; `.env.example` documents every variable

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "3.1", "5.1", "6.2"] },
    { "id": 2, "tasks": ["2.2", "3.2", "5.2", "6.1", "7.1"] },
    { "id": 3, "tasks": ["2.3", "3.3", "3.4", "3.5", "5.4", "6.3", "7.2"] },
    { "id": 4, "tasks": ["2.4", "2.5", "3.6", "3.7", "5.3", "7.3"] },
    { "id": 5, "tasks": ["8.1", "10.1", "10.2", "10.3"] },
    { "id": 6, "tasks": ["8.2", "9.1"] },
    { "id": 7, "tasks": ["8.3", "9.2"] },
    { "id": 8, "tasks": ["12.1"] },
    { "id": 9, "tasks": ["12.2", "12.3", "12.4"] }
  ]
}
```
