# Requirements Document

## Introduction

The e2e-skeleton feature establishes the foundational end-to-end data flow for the Real-Time Ride/Delivery Tracking & Dispatch Platform. The goal is to prove that a single driver GPS ping can travel through the full pipeline — from ingestion through dispatch logic to a rider notification — all running locally in docker-compose. This skeleton deliberately avoids Kubernetes, Flink, and production-grade push notification providers; it validates the core architecture before complexity is added.

The data flow is: **Driver ping → Kafka (KRaft mode) → Dispatch logic → `TripAssigned` Domain Event → structured JSON notification log line (with `trace_id`) written to stdout, captured by the container runtime and shippable to a centralised log store**.

## Glossary

- **Ingest_Service**: The Go service that receives GPS pings via HTTP and publishes `LocationPingReceived` Avro Domain Events to Kafka via Confluent Schema Registry.
- **Dispatch_Service**: The Spring Boot service that consumes `TripRequested` Domain Events, applies hardcoded nearest-driver logic, and publishes `TripAssigned` Domain Events. Owns the `Trip` aggregate.
- **Notification_Service**: The Go service that consumes `TripAssigned` Avro Domain Events from Kafka and writes structured JSON log lines (including `trace_id`) to stdout. Stdout is the correct output target for containerised services — the container runtime captures it and a log collector (Fluent Bit / Promtail) ships it to a centralised store (Loki / Elasticsearch). The `trace_id` field correlates each log line to the distributed trace in Jaeger.
- **Driver_Simulator**: The Python script that emits GPS pings at a configurable rate along a GeoJSON route.
- **Rider_UI**: The minimal React single-page application that displays a static map and allows a rider to request a ride.
- **Kafka_Broker**: The Apache Kafka 3-broker cluster running in KRaft mode as local Docker containers. KRaft eliminates ZooKeeper — each broker acts as both broker and controller (`KAFKA_PROCESS_ROLES=broker,controller`). Topics are configured with `replication.factor=3`, `min.insync.replicas=2`.
- **Schema_Registry**: The Confluent Schema Registry container that enforces Avro schema compatibility for all Domain Events. Producers register schemas on first publish; the broker rejects messages that violate the registered schema.
- **GPS_Ping**: A single location update message containing a driver identifier, latitude, longitude, and timestamp.
- **LocationPingReceived**: A Domain Event published to the `gps-pings` topic when the Ingest_Service receives a valid GPS_Ping. Payload contains `driver_id`, `latitude`, `longitude`, and `timestamp`.
- **TripRequested**: A Domain Event published to the `ride-events` topic when the Dispatch_Service HTTP endpoint receives a ride request. Payload contains `trip_id`, `rider_id`, `pickup_location`, and `requested_at`.
- **TripAssigned**: A Domain Event published to the `ride-events` topic when the Dispatch_Service assigns a driver to a trip. Payload contains `trip_id`, `driver_id`, `rider_id`, and `assigned_at`.
- **Trip_State_Machine**: The explicit state machine owned by the Dispatch_Service that tracks the lifecycle of a Trip aggregate. Phase 1 states: `REQUESTED`, `ASSIGNED`, `CANCELLED`. Full states defined in ADR 006.
- **Saga_State_Monitor**: A background job in the Dispatch_Service (Phase 2) that detects trips stuck in intermediate states beyond a timeout threshold and emits compensating domain events.
- **Ride_Request**: An HTTP request from the Rider_UI to the Dispatch_Service stub requesting a driver match. Results in a `TripRequested` Domain Event.
- **Compose_Environment**: The local development environment defined by `docker-compose.yml`, comprising all services and infrastructure containers.
- **OpenAPI_Spec**: The machine-readable API description (OpenAPI 3.x JSON/YAML) generated from and committed alongside each service that exposes HTTP endpoints — including both FastAPI services and the Spring Boot Dispatch_Service.
- **Domain_Event**: A named, past-tense record of something that happened within a bounded context, used as the Kafka integration contract between services. All Domain Events are serialised as **Avro** via Confluent Schema Registry. All Domain Events include `event_id` (UUID, unique per event instance), `event_type` (string), and `occurred_at` (ISO 8601 timestamp) fields in a standard envelope, with domain-specific data in a `payload` object. Avro schemas are defined in `shared/avro/`.
- **Gateway_Service**: The Go service that acts as a Kafka consumer (`gateway-consumer-group`) and WebSocket server. Kafka fans out Domain Events to it; its sole job is protocol translation — Kafka Domain Event → WebSocket frame pushed to the connected rider. It owns the WebSocket session registry (`rider_id → connection`). It does NOT fan out events; Kafka does that via consumer groups.
- **PgBouncer**: The connection pooler running in transaction pooling mode between the Dispatch_Service and PostgreSQL. Multiplexes application connections onto a small pool of actual database connections, preventing PostgreSQL connection exhaustion at scale.
- **DynamoDB_Local**: The local AWS DynamoDB emulator running as a Docker container. Used in Phase 2 for the Notification_Service `processed_events` deduplication table (`PK=event_id`, TTL-based expiry, atomic `ConditionExpression: attribute_not_exists(event_id)`). Modelled in Phase 1, activated in Phase 2.
- **Event_ID**: A UUID generated by the producer at publish time, unique per Domain Event instance. Used as the deduplication key for consumer-side idempotency. The Outbox relay preserves the original `event_id` on republish.

---

## Requirements

### Requirement 1: Monorepo Directory Scaffold

**User Story:** As a developer, I want a consistent top-level directory structure, so that all services and infrastructure assets have a predictable home from day one.

#### Acceptance Criteria

1. THE Monorepo SHALL contain a `services/` directory with subdirectories `ingest/`, `dispatch/`, `notification/`, `tracking/`, and `gateway/`.
2. THE Monorepo SHALL contain an `infra/` directory with subdirectories `docker/`, `k8s/`, and `kafka/`.
3. THE Monorepo SHALL contain a `scripts/` directory for developer and operational helper scripts.
4. THE Monorepo SHALL contain a `shared/` directory restricted to infrastructure concerns only: Kafka message envelope schema, health check DTOs, common error shapes, and proto/Avro definitions. Domain objects SHALL NOT be placed in `shared/`.
5. THE Monorepo SHALL contain a root-level `docker-compose.yml` that references all services defined in this skeleton.
6. IF a developer clones the repository and runs `docker-compose up`, THEN THE Compose_Environment SHALL start without manual directory creation steps.
7. THE Monorepo SHALL contain a `.env.example` file at the repository root documenting every required environment variable with placeholder values. THE `.env` file SHALL be gitignored and SHALL NOT be committed to source control.

---

### Requirement 2: Go Location Ingestion Service

**User Story:** As a driver simulator or mobile client, I want to POST GPS pings to an HTTP endpoint, so that my location is captured and forwarded into the processing pipeline.

#### Acceptance Criteria

1. THE Ingest_Service SHALL expose a `POST /location` HTTP endpoint accepting a JSON body containing `driver_id` (string), `latitude` (float, −90 to 90), `longitude` (float, −180 to 180), and `timestamp` (ISO 8601 string).
2. WHEN a valid GPS_Ping is received, THE Ingest_Service SHALL publish a `LocationPingReceived` Domain Event as an **Avro-serialised message** to the Kafka `gps-pings` topic within 500 ms of receiving the HTTP request. The message SHALL conform to the standard Domain Event envelope registered in the Schema_Registry: `event_id` (UUID, generated at publish time), `event_type` (set to `"LocationPingReceived"`), `occurred_at` (ISO 8601 timestamp), and `payload` containing `driver_id`, `latitude`, `longitude`, and `timestamp`.
3. WHEN the `gps-pings` topic is unavailable, THE Ingest_Service SHALL return HTTP 503 and SHALL NOT silently drop the ping.
4. IF the request body is missing any required field, THEN THE Ingest_Service SHALL return HTTP 422 with a structured error body identifying the missing fields.
5. IF `latitude` is outside the range −90 to 90 or `longitude` is outside the range −180 to 180, THEN THE Ingest_Service SHALL return HTTP 422.
6. THE Ingest_Service SHALL return HTTP 202 with a JSON body containing a `message_id` field (equal to the `event_id` of the published Domain Event) upon successful Kafka publish.
7. THE Ingest_Service SHALL generate and commit an OpenAPI_Spec file at `services/ingest/openapi.json` that accurately describes the `POST /location` endpoint, its request schema, and all response codes.
8. THE Ingest_Service SHALL read all configuration values (Kafka broker address, Schema Registry URL, topic name, service port) from environment variables with no hardcoded defaults pointing to external hosts.
9. THE Ingest_Service Kafka producer SHALL be configured with `acks=all` and `enable.idempotence=true` to prevent duplicate message delivery within a producer session.
10. THE Ingest_Service SHALL enforce a maximum request body size of 64 KB on `POST /location`, returning HTTP 413 if exceeded.
11. THE Ingest_Service Dockerfile SHALL use a multi-stage build (`golang:1.22-alpine` build stage → `gcr.io/distroless/static-debian12` final stage), run the service process as a non-root user, and use pinned base image versions (not `latest`).
12. THE Ingest_Service SHALL expose a `GET /metrics` endpoint in Prometheus text format and instrument all HTTP handlers with OpenTelemetry traces. The `trace_id` SHALL be propagated into Kafka message headers (`traceparent` W3C format).

---

### Requirement 3: Spring Boot Dispatch Service Stub

**User Story:** As the platform, I want a dispatch service that reacts to ride requests and assigns a driver, so that the end-to-end event flow can be validated without a full matching engine.

#### Acceptance Criteria

1. THE Dispatch_Service SHALL consume messages from the Kafka `ride-events` topic using a Kafka consumer group, filtering for messages with `event_type` equal to `"TripRequested"`.
2. WHEN a `TripRequested` Domain Event is consumed, THE Dispatch_Service SHALL apply hardcoded nearest-driver logic that selects a driver identifier from a static in-memory list.
3. WHEN a driver is selected, THE Dispatch_Service SHALL publish a `TripAssigned` Domain Event to the `ride-events` topic as an **Avro-serialised message** conforming to the standard envelope registered in the Schema_Registry: `event_id` (UUID, generated at publish time), `event_type` (set to `"TripAssigned"`), `occurred_at` (ISO 8601 timestamp), and `payload` containing `trip_id` (UUID), `driver_id` (string), `rider_id` (string), and `assigned_at` (ISO 8601 timestamp).
4. THE Dispatch_Service SHALL also expose a `POST /request-ride` HTTP endpoint that accepts a JSON body with `rider_id` (string) and `pickup_location` (object with `latitude` and `longitude`), publishes a `TripRequested` Avro Domain Event to `ride-events` conforming to the standard envelope with `payload` containing `trip_id`, `rider_id`, `pickup_location`, and `requested_at`, and returns HTTP 202 with a `trip_id`.
5. IF the Kafka broker is unreachable at startup, THEN THE Dispatch_Service SHALL log the error and retry the connection with exponential backoff up to 5 attempts before exiting with a non-zero status code.
6. THE Dispatch_Service SHALL read all configuration values (Kafka broker address, topic names, service port) from environment variables.
7. WHILE processing a `TripRequested` Domain Event, THE Dispatch_Service SHALL complete dispatch and publish the `TripAssigned` Domain Event within 2 seconds.
8. THE Dispatch_Service SHALL include the `springdoc-openapi` dependency and expose a `/v3/api-docs` endpoint that generates an OpenAPI 3.x spec describing the `POST /request-ride` endpoint, its request schema, and all response codes.
9. THE Dispatch_Service SHALL generate and commit an OpenAPI_Spec file at `services/dispatch/openapi.json` that is exported from the `/v3/api-docs` endpoint.
10. THE Dispatch_Service Kafka producer SHALL be configured with `acks=all` and `enable.idempotence=true` to prevent duplicate message delivery within a producer session.
11. THE Dispatch_Service SHALL enforce a maximum request body size of 64 KB on `POST /request-ride`, returning HTTP 413 if exceeded.
12. THE Dispatch_Service Dockerfile SHALL run the service process as a non-root user and SHALL use a pinned base image version (not `latest`).
13. THE Dispatch_Service SHALL consume messages from the Kafka `gps-pings` topic using a dedicated Kafka consumer group (`dispatch-location-group`), separate from the Tracking service's consumer group. This consumer is the stub for the CQRS local read model (see ADR 005).
14. WHEN a `LocationPingReceived` Domain Event is consumed from `gps-pings`, THE Dispatch_Service SHALL validate the event envelope (asserting `event_type` equals `"LocationPingReceived"` and `event_id` is a non-empty UUID) and log receipt to stdout at DEBUG level. In Phase 1, THE Dispatch_Service SHALL NOT yet update a Redis geospatial index — the Redis GEOADD logic is deferred to Phase 2.
15. IF a message consumed from `gps-pings` cannot be deserialized or fails envelope validation, THE Dispatch_Service SHALL log a warning to stderr and continue consuming subsequent messages without crashing.
16. THE Dispatch_Service SHALL persist each Trip as a record in PostgreSQL with a `status` column representing the Trip state machine. In Phase 1, valid statuses are `REQUESTED`, `ASSIGNED`, and `CANCELLED`. The `status` SHALL be set to `REQUESTED` when the `TripRequested` event is published and updated to `ASSIGNED` when the `TripAssigned` event is published.
17. THE Dispatch_Service SHALL model `TripCancelled` as a domain event in code (event type, envelope schema) even though it is not triggered in Phase 1. This ensures the compensating event exists in the domain model before Phase 2 adds the Saga State Monitor.

---

### Requirement 4: Go Notification Service Stub

**User Story:** As a developer, I want a notification service that writes structured JSON log lines (with `trace_id`) to stdout for assigned-trip events, so that I can verify the full pipeline end-to-end, correlate logs to distributed traces in Jaeger, and ship logs to a centralised store without requiring real push notification credentials.

#### Acceptance Criteria

1. THE Notification_Service SHALL consume Avro-serialised messages from the Kafka `ride-events` topic using a dedicated Kafka consumer group distinct from the Dispatch_Service consumer group.
2. WHEN a `TripAssigned` Domain Event is consumed, THE Notification_Service SHALL log a structured JSON line to stdout containing `event_id`, `event_type`, `trip_id`, `driver_id`, `rider_id`, `assigned_at`, a `notification_sent_at` timestamp, and a `trace_id` field.
3. THE Notification_Service SHALL filter consumed messages and SHALL only act on messages whose `event_type` is `"TripAssigned"`; all other event types SHALL be acknowledged and skipped without logging an error.
4. IF a consumed message cannot be deserialised from Avro, THEN THE Notification_Service SHALL log a warning to stderr including the raw message bytes and SHALL continue consuming subsequent messages.
5. THE Notification_Service SHALL generate and commit an OpenAPI_Spec file at `services/notification/openapi.json` describing any HTTP endpoints it exposes (health check and metrics at minimum).
6. THE Notification_Service SHALL expose a `GET /health` endpoint returning HTTP 200 with `{"status": "ok"}` and a `GET /metrics` endpoint in Prometheus text format.
7. THE Notification_Service SHALL read all configuration values (Kafka broker address, Schema Registry URL, topic name, consumer group ID, service port) from environment variables.
8. THE Notification_Service log output SHALL be idempotent with respect to duplicate `TripAssigned` deliveries in Phase 1 — logging the same `event_id` more than once is acceptable. Consumer-side deduplication via a DynamoDB `processed_events` table is deferred to Phase 2 (see ADR 003).
9. THE Notification_Service Dockerfile SHALL use a multi-stage build (`golang:1.22-alpine` build stage → `gcr.io/distroless/static-debian12` final stage), run the service process as a non-root user, and use pinned base image versions (not `latest`).

---

### Requirement 5: Driver Simulator Script

**User Story:** As a developer, I want a script that simulates a driver emitting GPS pings, so that I can exercise the full pipeline without a physical device.

#### Acceptance Criteria

1. THE Driver_Simulator SHALL be a Python script located at `scripts/simulate_driver.py`.
2. THE Driver_Simulator SHALL accept command-line arguments for `--driver-id` (string), `--route-file` (path to a GeoJSON LineString file), `--rate` (pings per second, default 10), and `--ingest-url` (base URL of the Ingest_Service).
3. WHEN executed, THE Driver_Simulator SHALL interpolate positions along the GeoJSON route and emit GPS_Pings to the Ingest_Service `POST /location` endpoint at the configured rate.
4. THE Driver_Simulator SHALL emit pings at a rate of 10 GPS_Pings per second per simulated driver by default.
5. IF the Ingest_Service returns a non-2xx response, THEN THE Driver_Simulator SHALL log the error to stderr including the HTTP status code and response body, and SHALL continue emitting subsequent pings.
6. WHEN the end of the GeoJSON route is reached, THE Driver_Simulator SHALL loop back to the start of the route and continue emitting pings.
7. THE Driver_Simulator SHALL include a sample GeoJSON route file at `scripts/sample_route.geojson` containing at least 10 coordinate pairs.

---

### Requirement 6: Minimal React Rider UI

**User Story:** As a rider, I want a simple web page with a map and a "Request Ride" button, so that I can trigger the dispatch flow and observe the end-to-end pipeline in action.

#### Acceptance Criteria

1. THE Rider_UI SHALL be a React single-page application located at `services/gateway/` or a dedicated `services/rider-ui/` directory.
2. THE Rider_UI SHALL render a Leaflet map centred on a default coordinate pair with a zoom level that shows a city-scale area.
3. THE Rider_UI SHALL display a "Request Ride" button that, when clicked, sends a `POST /request-ride` HTTP request to the Dispatch_Service stub with a hardcoded `rider_id` and the map centre coordinates as `pickup_location`.
4. WHEN the Dispatch_Service returns HTTP 202, THE Rider_UI SHALL display the returned `trip_id` on screen.
5. IF the Dispatch_Service returns a non-2xx response, THEN THE Rider_UI SHALL display a human-readable error message on screen and SHALL NOT leave the user with a blank or crashed page.
6. THE Rider_UI SHALL be served as a static build from within the Compose_Environment; the Dispatch_Service URL SHALL be configurable via a build-time environment variable.

---

### Requirement 7: docker-compose Local Environment

**User Story:** As a developer, I want a single `docker-compose up` command to start the entire skeleton, so that I can validate the end-to-end flow on any machine without manual setup.

#### Acceptance Criteria

1. THE Compose_Environment SHALL include containers for: Apache Kafka (3-broker KRaft cluster), Confluent Schema Registry, PgBouncer, PostgreSQL, Redis, DynamoDB Local, Jaeger, Prometheus, Grafana, Ingest_Service, Dispatch_Service, Notification_Service, and Gateway_Service.
2. WHEN `docker-compose up` is executed from the repository root, THE Compose_Environment SHALL reach a healthy state with all services passing their health checks within 120 seconds on a standard developer laptop.
3. THE Compose_Environment SHALL define a 3-broker Kafka KRaft cluster that creates the Kafka topics `gps-pings`, `ride-events`, `dispatch-commands`, and `notifications` on first startup with `replication.factor=3` and `min.insync.replicas=2`.
4. THE Compose_Environment SHALL configure all inter-service hostnames and ports via environment variables injected into each service container; no service SHALL hardcode another service's hostname.
5. THE Compose_Environment SHALL define named Docker volumes for Redis and PostgreSQL data so that data persists across `docker-compose stop` / `docker-compose start` cycles.
6. WHEN any service container exits with a non-zero status code, THE Compose_Environment SHALL surface the failure in the `docker-compose up` output without silently restarting the failed container indefinitely.
7. THE Compose_Environment SHALL expose the Ingest_Service on host port 8001, the Dispatch_Service on host port 8080, the Notification_Service on host port 8002, and the Gateway_Service on host port 8003, so that the Driver_Simulator and Rider_UI can reach them from the host machine.
8. THE Compose_Environment SHALL define four named Docker networks: `kafka-net` (Kafka brokers + Schema Registry + all services), `db-net` (PostgreSQL + PgBouncer + Redis + DynamoDB Local + Dispatch_Service only), `frontend-net` (Dispatch_Service + Gateway_Service + Rider_UI only), and `observability-net` (Jaeger + Prometheus + Grafana + all services). No service SHALL be connected to a network it does not require.
9. THE Compose_Environment SHALL configure Kafka with SASL/PLAIN authentication enabled. Each service SHALL authenticate to Kafka using its own dedicated credentials loaded from environment variables. No service SHALL share credentials with another service.
10. THE Compose_Environment SHALL set non-empty passwords on PostgreSQL and Redis, loaded from environment variables. No service SHALL connect to PostgreSQL or Redis without a password.

---

### Requirement 8: OpenAPI Spec Generation and Commit

**User Story:** As a developer or API consumer, I want committed OpenAPI specs for all services that expose HTTP endpoints, so that contracts are documented and can be used for client generation or contract testing.

#### Acceptance Criteria

1. THE Ingest_Service SHALL auto-generate its OpenAPI_Spec at application startup and write it to `services/ingest/openapi.json`.
2. THE Notification_Service SHALL auto-generate its OpenAPI_Spec at application startup and write it to `services/notification/openapi.json`.
3. THE Dispatch_Service SHALL export its OpenAPI_Spec from the `/v3/api-docs` endpoint and commit it to `services/dispatch/openapi.json`.
4. THE OpenAPI_Spec for each service SHALL be valid OpenAPI 3.x and SHALL include all request schemas, response schemas, and HTTP status codes defined in Requirements 2, 3, and 4.
5. THE Monorepo SHALL include a script or Makefile target that regenerates all three OpenAPI_Spec files and exits with a non-zero code if any regenerated output differs from the committed version, enabling CI enforcement.
6. WHEN the OpenAPI_Spec files are committed, THE Monorepo SHALL contain them at the paths `services/ingest/openapi.json`, `services/dispatch/openapi.json`, and `services/notification/openapi.json`.

---

### Requirement 9: Go Gateway Service (WebSocket Hub)

**User Story:** As a rider, I want to receive real-time trip state updates over a WebSocket connection, so that I can see when my driver is assigned without polling.

#### Acceptance Criteria

1. THE Gateway_Service SHALL be a Go service that acts as a Kafka consumer (`gateway-consumer-group`) consuming from the `ride-events` topic.
2. THE Gateway_Service SHALL expose a `GET /ws` WebSocket endpoint that upgrades HTTP connections and registers the connected rider's session in an in-memory session registry keyed by `rider_id`.
3. WHEN a `TripAssigned` Domain Event is consumed from Kafka, THE Gateway_Service SHALL look up the `rider_id` in the session registry and push the event payload as a JSON frame over the rider's WebSocket connection.
4. THE Gateway_Service SHALL NOT fan out events — Kafka fans out to the `gateway-consumer-group`; the Gateway translates the Kafka event to a WebSocket frame for the specific connected rider.
5. THE Gateway_Service SHALL expose a `GET /health` endpoint returning HTTP 200 with `{"status": "ok"}` and a `GET /metrics` endpoint in Prometheus text format.
6. THE Gateway_Service Dockerfile SHALL use a multi-stage build (`golang:1.22-alpine` build stage → `gcr.io/distroless/static-debian12` final stage), run as a non-root user, and use pinned base image versions.

---

### Requirement 12: Observability Baseline

**User Story:** As a developer, I want distributed traces, metrics, and structured logs from day one, so that I can observe the full pipeline end-to-end and demonstrate production-grade observability.

#### Acceptance Criteria

1. ALL services SHALL instrument HTTP handlers and Kafka producers/consumers with OpenTelemetry traces. The `traceparent` and `tracestate` W3C headers SHALL be injected into Kafka message headers by producers and extracted by consumers, maintaining the distributed trace across the async boundary.
2. ALL services SHALL expose a `GET /metrics` endpoint in Prometheus text format, including: HTTP request latency (p50/p95/p99), Kafka consumer lag, and service-specific metrics (e.g., GPS pings/sec for Ingest, active WebSocket connections for Gateway).
3. ALL service log lines SHALL be structured JSON and SHALL include a `trace_id` field so logs correlate to traces.
4. THE Compose_Environment SHALL include Jaeger (traces, host port 16686), Prometheus (metrics, host port 9090), and Grafana (dashboards, host port 3000) containers.
5. THE Compose_Environment SHALL configure all services to export traces to Jaeger via OTLP and metrics to Prometheus via scrape.

---

### Requirement 10: Security Baseline

**User Story:** As a platform engineer, I want security controls baked into the skeleton from day one, so that the patterns established here are safe to build on and do not need to be retrofitted later.

#### Acceptance Criteria

1. THE Monorepo SHALL NOT contain any credentials, passwords, API keys, or secrets in any committed file. All secrets SHALL be loaded from environment variables at runtime.
2. THE `.env.example` file SHALL document every environment variable required to run the Compose_Environment, with placeholder values and comments explaining each variable's purpose.
3. THE `docker-compose.yml` SHALL reference all credentials via `${VAR_NAME}` environment variable substitution — no hardcoded passwords or usernames.
4. THE Compose_Environment SHALL configure Kafka with SASL/PLAIN authentication. Each service SHALL have its own Kafka username and password with ACLs restricted to only the topics it needs to produce or consume (see ADR 004).
5. THE Compose_Environment SHALL assign each service to only the Docker networks it requires: `kafka-net` for Kafka access, `db-net` for database access, `frontend-net` for external HTTP access. No service SHALL have access to networks it does not use.
6. ALL service Dockerfiles SHALL run the application process as a non-root user using a `USER` directive.
7. ALL service Dockerfiles SHALL use pinned base image versions — Go services use `golang:1.22-alpine` build stage → `gcr.io/distroless/static-debian12` final stage; Java service uses `eclipse-temurin:21-jre-jammy`; Rider UI uses `node:20.12-alpine` build stage → `nginx:1.25-alpine` serve stage — never floating tags such as `latest`.
8. THE Ingest_Service and Dispatch_Service SHALL enforce a maximum HTTP request body size of 64 KB, returning HTTP 413 for oversized payloads.
9. THE `driver_id` and `rider_id` fields in all HTTP request bodies SHALL be validated as non-empty strings with a maximum length of 128 characters; requests exceeding this limit SHALL return HTTP 422.
10. IF a service fails to load a required environment variable at startup, THEN THE service SHALL log a descriptive error message identifying the missing variable and SHALL exit with a non-zero status code rather than starting with an insecure default.

---

### Requirement 11: End-to-End Data Flow Validation

**User Story:** As a developer, I want to verify that a single driver ping travels the full pipeline and produces a logged notification, so that I have confidence the skeleton is wired correctly before adding complexity.

#### Acceptance Criteria

1. WHEN the Driver_Simulator emits one GPS_Ping to the Ingest_Service and a Ride_Request is submitted via the Rider_UI, THE Notification_Service SHALL write a `TripAssigned` structured JSON log line to stdout within 5 seconds of the Ride_Request being accepted by the Dispatch_Service. The log line SHALL include a `trace_id` field correlating it to the distributed trace for that request.
2. THE `trip_id` in the `TripAssigned` Domain Event logged by the Notification_Service SHALL match the `trip_id` returned in the HTTP 202 response from the Dispatch_Service for the same Ride_Request. The `event_id` field SHALL be present and non-empty in the logged output.
3. WHEN the Compose_Environment is running and the Driver_Simulator is active, THE Ingest_Service SHALL accept GPS_Pings at a sustained rate of at least 10 pings per second without returning HTTP 5xx errors.
4. THE Compose_Environment SHALL include a `scripts/smoke_test.sh` script that: starts the Driver_Simulator for 5 seconds, submits one Ride_Request, and asserts that a matching `TripAssigned` Domain Event log line appears in the Notification_Service container logs within 10 seconds.
5. IF the smoke test script does not find the expected log line within 10 seconds, THEN THE smoke test script SHALL exit with a non-zero status code and print a diagnostic message identifying which stage of the pipeline did not produce output.
