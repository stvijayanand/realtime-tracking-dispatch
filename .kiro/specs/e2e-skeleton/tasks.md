# Implementation Plan: e2e-skeleton

## Overview

This plan converts the e2e-skeleton design into incremental coding tasks that build the full GPS-ping-to-notification pipeline. Each task builds on the previous, ending with all services wired together in docker-compose. The stack is: Go 1.22 (Ingest, Notification, Gateway), Java 21 / Spring Boot 3.x (Dispatch), Python 3.11 (Driver Simulator), React 18 TypeScript (Rider UI), 3-broker Kafka KRaft cluster with Confluent Schema Registry, PgBouncer → PostgreSQL, DynamoDB Local, and OpenTelemetry → Jaeger + Prometheus + Grafana.

## Tasks

- [x] 1. Monorepo scaffold, shared Avro schemas, and envelope types
  - [x] 1.1 Create top-level directory structure and root configuration files
    - Create `services/ingest/`, `services/dispatch/`, `services/notification/`, `services/tracking/`, `services/gateway/` directories
    - Create `infra/docker/`, `infra/k8s/`, `infra/kafka/`, `infra/terraform/` directories
    - Create `scripts/`, `shared/avro/`, `shared/envelope/`, `shared/proto/`, `docs/adr/`, `docs/query-plans/` directories
    - Create root `.env.example` documenting every required environment variable with placeholder values and inline comments; ensure `.env` is listed in `.gitignore`
    - Create root `Makefile` with targets: `build`, `test`, `lint`, `up`, `down`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 10.1, 10.2_

  - [x] 1.2 Write Avro schemas for all Domain Events in `shared/avro/`
    - Write `shared/avro/location_ping_received.avsc` — envelope fields `event_id`, `event_type`, `occurred_at`, `payload` (with `driver_id`, `latitude`, `longitude`, `timestamp`)
    - Write `shared/avro/trip_requested.avsc` — envelope + payload (`trip_id`, `rider_id`, `pickup_location`, `requested_at`)
    - Write `shared/avro/trip_assigned.avsc` — envelope + payload (`trip_id`, `driver_id`, `rider_id`, `assigned_at`)
    - Write `shared/avro/trip_cancelled.avsc` — envelope + payload (`trip_id`, `reason`, `cancelled_at`); modelled in Phase 1, not triggered
    - All schemas must use the standard envelope structure; `event_type` is part of the schema contract (not a Kafka header)
    - _Requirements: 1.4, 2.2, 3.3, 3.17_

  - [x] 1.3 Implement Go shared envelope package (`shared/envelope/envelope.go`)
    - Define `DomainEventEnvelope` struct with `avro` struct tags: `EventID`, `EventType`, `OccurredAt`, `Payload map[string]interface{}`
    - Implement `Validate(e DomainEventEnvelope) error` — checks `EventID` is a non-empty UUID string and `EventType` is non-empty; returns `EnvelopeValidationError` identifying the failing field
    - Do NOT place any domain types (`Trip`, `DriverLocation`, `Notification`) in `shared/`
    - _Requirements: 1.4_

  - [x] 1.4 Implement Java shared envelope type (`shared/KafkaEnvelope.java`)
    - Define `KafkaEnvelope` as a Java `record` with fields: `eventId`, `eventType`, `occurredAt`, `payload Map<String,Object>`
    - Implement static factory `KafkaEnvelope.of(String eventType, Map<String,Object> payload)` that generates `eventId` (UUID4) and `occurredAt` (ISO 8601)
    - Add Javadoc noting this factory is for infrastructure-level use only; service-specific domain events use `EventEnvelopeFactory`
    - _Requirements: 1.4_


- [x] 2. Go Ingest Service
  - [x] 2.1 Scaffold Ingest Service module structure and config
    - Initialise Go module at `services/ingest/` (`go mod init`)
    - Add dependencies: `github.com/go-chi/chi/v5`, `github.com/confluentinc/confluent-kafka-go/v2`, `github.com/go-playground/validator/v10`, `github.com/google/uuid`, `go.opentelemetry.io/otel`, `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`, `pgregory.net/rapid` (test-only)
    - Implement `config/config.go`: `Config` struct with fields `KafkaBootstrapServers`, `KafkaTopic`, `KafkaSASLUsername`, `KafkaSASLPassword`, `SchemaRegistryURL`, `ServicePort`, `OTELEndpoint`; implement `LoadConfig()` reading each via `os.Getenv`; log descriptive error identifying the missing variable and call `os.Exit(1)` if any required var is absent
    - _Requirements: 2.8, 10.10_

  - [x] 2.2 Implement Ingest Service domain model and event factory
    - Implement `model/gps_ping.go`: `GpsPingRequest` struct with `go-playground/validator` tags — `DriverID` (required, max=128), `Latitude` (required, min=-90, max=90), `Longitude` (required, min=-180, max=180), `Timestamp` (required)
    - Implement `events/location_ping.go`: `DomainEvent` struct; `BuildLocationPingEvent(ping model.GpsPingRequest) DomainEvent` factory — generates `EventID` (UUID4 via `github.com/google/uuid`), sets `EventType = "LocationPingReceived"`, sets `OccurredAt` to `time.Now().UTC()` ISO 8601, copies all ping fields into `Payload`; never returns an error
    - _Requirements: 2.1, 2.2_

  - [x] 2.3 Implement Ingest Service Kafka producer (`kafka/producer.go`)
    - Implement `Producer` struct with fields `producer *confluent.Producer`, `schemaRegistryURL string`, `topic string`
    - Implement `NewProducer(cfg config.Config) (*Producer, error)` — configure with `acks=all`, `enable.idempotence=true`, SASL/PLAIN credentials from config
    - Implement `Publish(key string, event events.DomainEvent) (string, error)` — serialise event as Avro via Schema Registry client (register schema from `shared/avro/location_ping_received.avsc` on first publish), call `producer.Produce()`, flush; return `event_id` on success; return error on delivery failure (caller returns HTTP 503); inject `traceparent` W3C header into Kafka message headers
    - _Requirements: 2.2, 2.3, 2.9, 12.1_

  - [x] 2.4 Implement Ingest Service HTTP handlers and middleware
    - Implement `middleware/body_size.go`: `MaxBodySize(limit int64) func(http.Handler) http.Handler` — standard Go middleware that returns HTTP 413 if request body exceeds `limit` bytes before the handler is called; limit is 65536 (64 KB)
    - Implement `handler/health.go`: `GET /health` handler returning `200 {"status": "ok"}`
    - Implement `handler/location.go`: `LocationHandler` struct with `Producer *kafka.Producer` field (constructor injection — never instantiate producer inside handler body); `ServeHTTP` method: decode JSON → `GpsPingRequest`, validate struct tags (return 422 with structured error body on failure), call `BuildLocationPingEvent`, call `Producer.Publish(key=driver_id, event)`, return `202 {"message_id": event_id}` on success or `503` on Kafka error
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.6, 2.10, 10.8, 10.9_

  - [x] 2.5 Implement Ingest Service main entrypoint with OTel and metrics
    - Implement `main.go`: initialise OTel tracer (OTLP exporter to `OTEL_EXPORTER_OTLP_ENDPOINT`), call `config.LoadConfig()`, construct `kafka.NewProducer(cfg)`, construct `LocationHandler{Producer: p}`, build `chi` router with middleware chain (`MaxBodySize(65536)` → `otelhttp.NewHandler` → routes), register `GET /health` and `GET /metrics` (Prometheus text format) routes, start HTTP server on `cfg.ServicePort`, implement graceful shutdown on SIGTERM/SIGINT
    - _Requirements: 2.8, 2.12, 12.1, 12.2, 12.3_

  - [x] 2.6 Write Ingest Service Dockerfile
    - Write `infra/docker/ingest.Dockerfile` (or `services/ingest/Dockerfile`): multi-stage build — `FROM golang:1.22-alpine AS builder` compiles static binary with `CGO_ENABLED=0`; `FROM gcr.io/distroless/static-debian12` final stage copies only the binary; add `USER nonroot:nonroot` directive; use pinned digest versions, never `latest`
    - _Requirements: 2.11, 10.6, 10.7_

  - [ ]* 2.7 Write property test for GPS ping event envelope round-trip (Property 1)
    - File: `services/ingest/tests/location_handler_test.go`
    - `// Feature: e2e-skeleton, Property 1: GPS ping event envelope round-trip`
    - Use `pgregory.net/rapid` to generate: `driver_id` (1–128 chars), `latitude` in [−90, 90], `longitude` in [−180, 180], ISO 8601 `timestamp`
    - Assert: `message_id` in HTTP 202 response equals `event_id` in the Kafka message captured by mock producer; Kafka message has `event_type = "LocationPingReceived"`; payload preserves all four input fields exactly
    - Mock the Kafka producer using confluent-kafka-go mock producer; minimum 100 iterations
    - **Property 1: GPS ping event envelope round-trip**
    - **Validates: Requirements 2.2, 2.6**

  - [ ]* 2.8 Write property test for Ingest Service invalid input rejection (Property 2)
    - File: `services/ingest/tests/location_handler_test.go`
    - `// Feature: e2e-skeleton, Property 2: Ingest Service rejects invalid GPS ping inputs`
    - Use `pgregory.net/rapid` to generate requests with: missing required fields, `latitude` outside [−90, 90], `longitude` outside [−180, 180], empty `driver_id`, `driver_id` > 128 chars
    - Assert: service returns HTTP 422 for all invalid inputs; mock producer `Publish` is never called
    - **Property 2: Ingest Service rejects invalid GPS ping inputs**
    - **Validates: Requirements 2.4, 2.5, 10.9**

  - [ ]* 2.9 Write property test for 64 KB body size enforcement on Ingest Service (Property 5)
    - File: `services/ingest/tests/location_handler_test.go`
    - `// Feature: e2e-skeleton, Property 5: HTTP request body size enforcement`
    - Use `pgregory.net/rapid` to generate payloads of varying byte sizes around the 64 KB boundary (e.g., 65535, 65536, 65537, random sizes up to 128 KB)
    - Assert: bodies > 65536 bytes return HTTP 413; bodies ≤ 65536 bytes that are otherwise valid return HTTP 202
    - **Property 5: HTTP request body size enforcement**
    - **Validates: Requirements 2.10, 10.8**


- [x] 3. Spring Boot Dispatch Service
  - [x] 3.1 Scaffold Dispatch Service project structure and domain model
    - Initialise Spring Boot 3.x project at `services/dispatch/` with dependencies: `spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-kafka`, `io.confluent:kafka-avro-serializer`, `springdoc-openapi-starter-webmvc-ui`, `flyway-core`, `postgresql`, `opentelemetry-spring-boot-starter`, `jqwik` (test-only)
    - Implement `domain/TripStatus.java` enum: `REQUESTED`, `ASSIGNED`, `CANCELLED`; implement `assertCanTransitionTo(TripStatus next)` guard method using `VALID_TRANSITIONS` map; throw `IllegalStateTransitionException(this, next)` on invalid transition
    - Implement `domain/Trip.java` `@Entity`: fields `tripId` (UUID PK), `riderId` (VARCHAR 128), `driverId` (VARCHAR 128, nullable), `status` (VARCHAR 20), `pickupLat`, `pickupLng` (DOUBLE PRECISION), `requestedAt`, `assignedAt` (nullable), `updatedAt` (TIMESTAMPTZ DEFAULT now())
    - Implement `domain/TripRepository.java` extending `JpaRepository<Trip, UUID>`
    - Write Flyway migration `V1__create_trips_table.sql` in `src/main/resources/db/migration/`: CREATE TABLE trips + `idx_trips_status` + `idx_trips_updated_at` indexes; commit `EXPLAIN ANALYZE` output to `docs/query-plans/V1_trips_indexes.md`
    - _Requirements: 3.16, 3.17_

  - [x] 3.2 Implement Dispatch Service configuration and fail-fast startup
    - Implement `config/AppConfig.java` `@Configuration`: `@PostConstruct validateRequiredEnvVars()` iterates required env var names (`KAFKA_BOOTSTRAP_SERVERS`, `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `SCHEMA_REGISTRY_URL`, `SERVICE_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`) and throws `IllegalStateException` with the missing variable name if any are absent
    - Implement `config/KafkaConsumerConfig.java`: consumer factory beans for `ride-events` (`dispatch-consumer-group`) and `gps-pings` (`dispatch-location-group`); configure Avro deserialiser with Schema Registry URL; configure exponential backoff retry (up to 5 attempts) on startup connection failure
    - _Requirements: 3.5, 3.6, 10.10_

  - [x] 3.3 Implement Dispatch Service domain events and factory
    - Implement `events/DomainEventEnvelope.java` record: `eventId`, `eventType`, `occurredAt`, `payload Map<String,Object>`
    - Implement `events/TripRequestedPayload.java`, `TripAssignedPayload.java`, `TripCancelledPayload.java` records with all required fields
    - Implement `events/EventEnvelopeFactory.java` static factory: `buildTripRequested(UUID tripId, String riderId, PickupLocation pickup, Instant requestedAt)` and `buildTripAssigned(UUID tripId, String driverId, String riderId, Instant assignedAt)` — all UUID generation and timestamp generation isolated here; `DispatchService` never calls `UUID.randomUUID()` directly
    - _Requirements: 3.3, 3.4_

  - [x] 3.4 Implement Dispatch Service driver selection strategy and service layer
    - Implement `service/DriverSelectionStrategy.java` interface: `String selectDriver(PickupLocation pickup)`
    - Implement `service/HardcodedDriverSelectionStrategy.java` `@Component`: static list `["driver-001", "driver-002", "driver-003"]`; round-robin via `AtomicInteger`; ignores `pickup` in Phase 1
    - Implement `service/DispatchService.java` `@Service`: `@Transactional requestRide(String riderId, PickupLocation pickup)` — generate `tripId`, persist `Trip(status=REQUESTED)`, build `TripRequested` envelope via `EventEnvelopeFactory`, publish to `ride-events`, return `tripId`; `@Transactional assignDriver(UUID tripId, String riderId)` — load Trip, call `status.assertCanTransitionTo(ASSIGNED)`, call `driverSelectionStrategy.selectDriver(pickup)`, update Trip, build `TripAssigned` envelope, publish to `ride-events`; Kafka producer configured with `acks=all`, `enable.idempotence=true`
    - _Requirements: 3.1, 3.2, 3.3, 3.7, 3.10_

  - [x] 3.5 Implement Dispatch Service Kafka consumers
    - Implement `consumer/EnvelopeValidator.java` stateless validator: `static void validate(DomainEventEnvelope envelope)` — asserts `eventId` is non-empty UUID string and `eventType` is non-null/non-empty; throws `EnvelopeValidationException` with descriptive message on failure
    - Implement `consumer/RideEventsConsumer.java` `@KafkaListener` on `ride-events` topic, group `dispatch-consumer-group`: deserialise Avro → `DomainEventEnvelope`; filter `event_type == "TripRequested"` only; delegate to `dispatchService.assignDriver(tripId, riderId)`; must complete within 2 seconds; propagate W3C `traceparent` header from Kafka message headers to OTel span context
    - Implement `consumer/LocationPingConsumer.java` `@KafkaListener` on `gps-pings` topic, group `dispatch-location-group`: call `EnvelopeValidator.validate()`; log receipt at DEBUG level; on deserialisation failure or envelope validation failure: log WARNING to stderr, commit offset, continue; do NOT write to Redis in Phase 1
    - _Requirements: 3.1, 3.13, 3.14, 3.15, 12.1_

  - [x] 3.6 Implement Dispatch Service HTTP layer and OpenAPI
    - Implement `web/dto/PickupLocation.java`, `RequestRideRequest.java` (record with `@Valid` annotations: `riderId` non-empty max 128, `pickupLocation` non-null), `RequestRideResponse.java` (record: `tripId UUID`)
    - Implement `web/RideController.java` `@RestController`: `POST /request-ride` — `@Valid` request body, enforce 64 KB body size limit (Spring `spring.servlet.multipart.max-request-size` or `@RequestBody` size filter), delegate to `dispatchService.requestRide()`, return `202 {"trip_id": tripId}`; return 413 for oversized body, 422 for validation failure
    - Implement `web/HealthController.java`: `GET /health` returning `200 {"status": "UP"}`
    - Configure `springdoc-openapi` to expose `/v3/api-docs`; auto-generate `services/dispatch/openapi.json` on startup
    - _Requirements: 3.4, 3.8, 3.9, 3.11, 10.8, 10.9_

  - [x] 3.7 Write Dispatch Service Dockerfile
    - Write `infra/docker/dispatch.Dockerfile` (or `services/dispatch/Dockerfile`): `FROM eclipse-temurin:21-jre-jammy` (pinned version); add `RUN groupadd -r appuser && useradd -r -g appuser appuser`; `USER appuser`; copy JAR; `ENTRYPOINT ["java", "-jar", "app.jar"]`
    - _Requirements: 3.12, 10.6, 10.7_

  - [ ]* 3.8 Write property test for TripAssigned envelope correctness (Property 3)
    - File: `services/dispatch/src/test/java/TripAssignedProducerTest.java`
    - `// Feature: e2e-skeleton, Property 3: TripAssigned event envelope correctness`
    - Use `jqwik` `@Property` to generate: random `trip_id` UUIDs, `rider_id` strings (1–128 chars), valid `pickup_location` coordinates
    - Assert: resulting `TripAssigned` event has non-empty `event_id` UUID, `event_type = "TripAssigned"`, valid ISO 8601 `occurred_at`, payload `trip_id` matches input, `driver_id` is from the static driver list, `rider_id` matches input, `assigned_at` is valid ISO 8601
    - Mock Kafka producer with Mockito; minimum 100 iterations
    - **Property 3: TripAssigned event envelope correctness**
    - **Validates: Requirements 3.3**

  - [ ]* 3.9 Write property test for Dispatch Service event type filtering (Property 4)
    - File: `services/dispatch/src/test/java/LocationPingConsumerTest.java`
    - `// Feature: e2e-skeleton, Property 4: Dispatch Service event type filtering`
    - Use `jqwik` `@Property` to generate random `event_type` strings excluding `"TripRequested"`
    - Assert: `RideEventsConsumer` does NOT call `dispatchService.assignDriver()` and does NOT publish a `TripAssigned` event for any non-`TripRequested` event type
    - **Property 4: Dispatch Service event type filtering**
    - **Validates: Requirements 3.1**

  - [ ]* 3.10 Write property test for gps-pings envelope validation (Property 6)
    - File: `services/dispatch/src/test/java/LocationPingConsumerTest.java`
    - `// Feature: e2e-skeleton, Property 6: Dispatch Service gps-pings envelope validation`
    - Use `jqwik` `@Property` to generate: valid envelopes, envelopes with missing `event_id`, envelopes with wrong `event_type`, non-Avro byte sequences
    - Assert: invalid/malformed messages log a WARNING and do not crash the consumer; valid `LocationPingReceived` envelopes are logged at DEBUG level
    - **Property 6: Dispatch Service gps-pings envelope validation**
    - **Validates: Requirements 3.14, 3.15**

  - [ ]* 3.11 Write property test for Trip state machine persistence round-trip (Property 9)
    - File: `services/dispatch/src/test/java/TripRepositoryTest.java`
    - `// Feature: e2e-skeleton, Property 9: Trip state machine persistence round-trip`
    - Use `jqwik` `@Property` with H2 in-memory DB to generate: random `rider_id` strings, valid `pickup_location` coordinates
    - Assert: after `requestRide()`, `trips` table contains record with matching `trip_id` and `status = 'REQUESTED'`; after `assignDriver()`, same record has `status = 'ASSIGNED'`, non-null `driver_id`, non-null `assigned_at`
    - **Property 9: Trip state machine persistence round-trip**
    - **Validates: Requirements 3.16**

  - [ ]* 3.12 Write property test for Dispatch Service startup fails fast on missing env vars (Property 12)
    - File: `services/dispatch/src/test/java/RequestRideEndpointTest.java`
    - `// Feature: e2e-skeleton, Property 12: Dispatch Service startup fails fast on missing environment variables`
    - Use `jqwik` `@Property` to omit each required env var in turn (`KAFKA_BOOTSTRAP_SERVERS`, `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`)
    - Assert: `AppConfig.validateRequiredEnvVars()` throws `IllegalStateException` with a message identifying the missing variable name; application context does not finish loading
    - **Property 12: Dispatch Service startup fails fast on missing environment variables**
    - **Validates: Requirements 10.10**


- [x] 4. Checkpoint — Ingest and Dispatch tests pass
  - Ensure all tests in `services/ingest/tests/` and `services/dispatch/src/test/java/` pass, ask the user if questions arise.

- [x] 5. Go Notification Service
  - [x] 5.1 Scaffold Notification Service module structure and config
    - Initialise Go module at `services/notification/` (`go mod init`)
    - Add dependencies: `github.com/confluentinc/confluent-kafka-go/v2`, `github.com/rs/zerolog` (or `log/slog`), `go.opentelemetry.io/otel`, `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`, `pgregory.net/rapid` (test-only), `github.com/stretchr/testify`
    - Implement `config/config.go`: `Config` struct with `KafkaBootstrapServers`, `KafkaTopic`, `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaConsumerGroupID`, `SchemaRegistryURL`, `ServicePort`, `OTELEndpoint`; `LoadConfig()` reads via `os.Getenv`; logs descriptive error identifying the missing variable and calls `os.Exit(1)` on any missing required var
    - _Requirements: 4.7, 10.10_

  - [x] 5.2 Implement Notification Service structured logger (`logger/logger.go`)
    - Implement `logger.Logger` struct wrapping zerolog (or slog) configured to emit JSON to stdout
    - Implement `LogNotification(event events.TripAssignedEvent)` method — writes one JSON line with fields: `event_id`, `event_type`, `trip_id`, `driver_id`, `rider_id`, `assigned_at`, `notification_sent_at` (`time.Now().UTC()` ISO 8601), `trace_id` (extracted from OTel span context)
    - Implement `LogWarning(msg string, rawBytes []byte)` method — writes JSON warning line to stderr with `trace_id`
    - All log output MUST go through `Logger` methods — never `fmt.Println()` or raw `log` calls
    - _Requirements: 4.2, 12.3_

  - [x] 5.3 Implement Notification Service domain event parser (`events/trip_assigned.go`)
    - Implement `TripAssignedEvent` struct with unexported fields: `eventID`, `eventType`, `tripID`, `driverID`, `riderID`, `assignedAt`
    - Implement accessor methods: `EventID()`, `TripID()`, `DriverID()`, `RiderID()`, `AssignedAt()`
    - Implement `ParseTripAssigned(envelope map[string]interface{}) (TripAssignedEvent, error)` — validates all required fields are present and non-empty; returns `EventParseError` (with field name) if any required field is absent; never returns a partially-constructed `TripAssignedEvent`
    - _Requirements: 4.2, 4.4_

  - [x] 5.4 Implement Notification Service handlers and consumer worker
    - Implement `handler/trip_assigned.go`: `HandleTripAssigned(event events.TripAssignedEvent, log *logger.Logger)` — calls `log.LogNotification(event)`; `NoOpHandler` no-op `HandlerFunc` for all other event types
    - Implement `consumer/worker.go`: `HandlerFunc` type `func(envelope map[string]interface{})`; `Worker` struct with `consumer *confluent.Consumer`, `handlers map[string]HandlerFunc`, `stopCh chan struct{}`; `NewWorker(cfg config.Config, handlers map[string]HandlerFunc) (*Worker, error)` — configure consumer with SASL/PLAIN, Schema Registry Avro deserialiser; `Start()` launches `run()` goroutine; `Stop()` signals stop and waits; `run()` loop: `Poll()` → Avro deserialise → extract `event_type` → `handlers[eventType](envelope)` (fallback to `NoOpHandler`) → `CommitOffsets()`; on Avro deserialisation failure: call `log.LogWarning()` with raw bytes, commit offset, continue; extract W3C `traceparent` header from Kafka message headers to create child OTel span
    - _Requirements: 4.1, 4.3, 4.4, 4.8, 12.1_

  - [x] 5.5 Implement Notification Service main entrypoint and HTTP server
    - Implement `main.go`: initialise OTel tracer, call `config.LoadConfig()`, construct `logger.Logger`, build handler map `{"TripAssigned": HandleTripAssigned, "*": NoOpHandler}`, construct `consumer.NewWorker(cfg, handlers)`, call `worker.Start()`, start HTTP server on `cfg.ServicePort` with `GET /health` (200 `{"status":"ok"}`) and `GET /metrics` (Prometheus text format) routes, auto-generate OpenAPI spec to `services/notification/openapi.json` at startup, implement graceful shutdown on SIGTERM/SIGINT calling `worker.Stop()`
    - _Requirements: 4.5, 4.6, 4.7, 8.2, 12.2_

  - [x] 5.6 Write Notification Service Dockerfile
    - Write `infra/docker/notification.Dockerfile` (or `services/notification/Dockerfile`): multi-stage — `FROM golang:1.22-alpine AS builder` with `CGO_ENABLED=0`; `FROM gcr.io/distroless/static-debian12` final stage; `USER nonroot:nonroot`; pinned versions, never `latest`
    - _Requirements: 4.9, 10.6, 10.7_

  - [ ]* 5.7 Write property test for Notification Service logs all required fields (Property 7)
    - File: `services/notification/tests/worker_test.go`
    - `// Feature: e2e-skeleton, Property 7: Notification Service logs all required fields for TripAssigned events`
    - Use `pgregory.net/rapid` to generate random `TripAssigned` payloads with varying `event_id`, `trip_id`, `driver_id`, `rider_id`, `assigned_at` values
    - Assert: structured JSON log line written to stdout contains all required fields: `event_id`, `event_type`, `trip_id`, `driver_id`, `rider_id`, `assigned_at`, `notification_sent_at`; capture stdout in test using a buffer writer injected into `logger.Logger`
    - **Property 7: Notification Service logs all required fields for TripAssigned events**
    - **Validates: Requirements 4.2**

  - [ ]* 5.8 Write property test for Notification Service filters non-TripAssigned events (Property 8)
    - File: `services/notification/tests/worker_test.go`
    - `// Feature: e2e-skeleton, Property 8: Notification Service filters non-TripAssigned events`
    - Use `pgregory.net/rapid` to generate random `event_type` strings excluding `"TripAssigned"`
    - Assert: `Worker` acknowledges the message, does NOT call `HandleTripAssigned`, does NOT write any line to stdout; `NoOpHandler` is invoked instead
    - **Property 8: Notification Service filters non-TripAssigned events**
    - **Validates: Requirements 4.3**


- [x] 6. Go Gateway Service
  - [x] 6.1 Scaffold Gateway Service module structure and config
    - Initialise Go module at `services/gateway/` (`go mod init`)
    - Add dependencies: `github.com/gorilla/websocket`, `github.com/confluentinc/confluent-kafka-go/v2`, `go.opentelemetry.io/otel`, `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`, `pgregory.net/rapid` (test-only), `github.com/stretchr/testify`
    - Implement `config/config.go`: `Config` struct with `KafkaBootstrapServers`, `KafkaTopic`, `KafkaSASLUsername`, `KafkaSASLPassword`, `KafkaConsumerGroupID`, `SchemaRegistryURL`, `ServicePort`, `OTELEndpoint`; `LoadConfig()` reads via `os.Getenv`; logs descriptive error identifying the missing variable and calls `os.Exit(1)` on any missing required var
    - _Requirements: 9.1, 10.10_

  - [x] 6.2 Implement Gateway Service session registry (`session/registry.go`)
    - Implement `Registry` struct with `mu sync.RWMutex` and `sessions map[string]*websocket.Conn` (Phase 1 in-memory; keyed by `rider_id`)
    - Implement `Register(riderID string, conn *websocket.Conn)` — acquires write lock, stores connection
    - Implement `Unregister(riderID string)` — acquires write lock, removes connection
    - Implement `Send(riderID string, payload []byte) error` — acquires read lock, looks up connection, calls `conn.WriteMessage(websocket.TextMessage, payload)`; returns error if rider not connected (no-op, not a crash)
    - _Requirements: 9.2, 9.4_

  - [x] 6.3 Implement Gateway Service WebSocket handler (`handler/websocket.go`)
    - Implement `WebSocketHandler` struct with `Registry *session.Registry` field (constructor injection)
    - Implement `ServeHTTP` for `GET /ws?rider_id=<string>`: validate `rider_id` query param (non-empty); upgrade HTTP connection using `gorilla/websocket` upgrader; call `registry.Register(riderID, conn)`; run read loop (heartbeat / ping-pong); on disconnect call `registry.Unregister(riderID)`
    - Implement `GET /health` handler returning `200 {"status": "ok"}`
    - _Requirements: 9.2, 9.5_

  - [x] 6.4 Implement Gateway Service Kafka consumer worker (`consumer/worker.go`)
    - Implement `Worker` struct with `consumer *confluent.Consumer`, `registry *session.Registry`, `stopCh chan struct{}`
    - Implement `NewWorker(cfg config.Config, registry *session.Registry) (*Worker, error)` — configure consumer with SASL/PLAIN, Schema Registry Avro deserialiser, consumer group `gateway-consumer-group`
    - Implement `Start()` / `Stop()` for graceful lifecycle management
    - Implement `run()` loop: `Poll()` → Avro deserialise → extract `event_type`; filter for `TripAssigned`, `TripCancelled`, `TripCompleted`; extract `rider_id` from payload; call `registry.Send(riderID, jsonPayload)`; commit offset; on Avro deserialisation failure: log warning, commit offset, continue; extract W3C `traceparent` header from Kafka message headers to create child OTel span
    - The Gateway does NOT fan out events — it translates one Kafka event to one WebSocket push for the specific connected rider
    - _Requirements: 9.1, 9.3, 9.4, 12.1_

  - [x] 6.5 Implement Gateway Service main entrypoint with OTel and metrics
    - Implement `main.go`: initialise OTel tracer, call `config.LoadConfig()`, construct `session.Registry`, construct `consumer.NewWorker(cfg, registry)`, call `worker.Start()`, build `chi` (or `net/http`) router with `GET /ws` → `WebSocketHandler`, `GET /health`, `GET /metrics` (Prometheus text format including active WebSocket connection count gauge), start HTTP server on `cfg.ServicePort`, implement graceful shutdown on SIGTERM/SIGINT
    - _Requirements: 9.5, 12.2, 12.3_

  - [x] 6.6 Write Gateway Service Dockerfile
    - Write `infra/docker/gateway.Dockerfile` (or `services/gateway/Dockerfile`): multi-stage — `FROM golang:1.22-alpine AS builder` with `CGO_ENABLED=0`; `FROM gcr.io/distroless/static-debian12` final stage; `USER nonroot:nonroot`; pinned versions, never `latest`
    - _Requirements: 9.6, 10.6, 10.7_


- [x] 7. Driver Simulator script (Python)
  - [x] 7.1 Implement Driver Simulator script and sample route
    - Write `scripts/simulate_driver.py` (Python 3.11, stdlib + `requests` only):
      - Accept CLI args: `--driver-id` (string), `--route-file` (path to GeoJSON LineString), `--rate` (pings/sec, default 10), `--ingest-url` (base URL)
      - Read and parse GeoJSON LineString from `--route-file`; exit non-zero with stderr error if file not found or not a valid GeoJSON LineString
      - Interpolate positions along the route at the configured rate; loop back to start when route end is reached
      - POST each position to `{ingest-url}/location` with JSON body `{"driver_id": ..., "latitude": ..., "longitude": ..., "timestamp": <ISO 8601>}`
      - On non-2xx response: log to stderr with HTTP status code and response body; continue emitting
      - On network timeout: log to stderr; continue emitting
    - Write `scripts/sample_route.geojson`: valid GeoJSON LineString with ≥10 coordinate pairs
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

  - [ ]* 7.2 Write property test for Driver Simulator route looping (Property 11)
    - File: `scripts/test_simulate_driver.py` (pytest)
    - `# Feature: e2e-skeleton, Property 11: Driver Simulator route looping`
    - Use `hypothesis` to generate valid GeoJSON LineString routes of varying lengths (2–100 coordinate pairs)
    - Assert: after the simulator emits a ping for the last coordinate, the next emitted ping has coordinates near the first coordinate of the route (within interpolation tolerance of 0.001 degrees)
    - Mock the HTTP POST call; do not require a running Ingest Service
    - **Property 11: Driver Simulator route looping**
    - **Validates: Requirements 5.6**

- [ ] 8. Minimal React Rider UI
  - [ ] 8.1 Scaffold Rider UI React application
    - Initialise React 18 TypeScript app at `services/rider-ui/` using Create React App or Vite with TypeScript template
    - Add dependencies: `react-leaflet`, `leaflet`, `@types/leaflet`
    - Configure `REACT_APP_DISPATCH_URL` as a build-time environment variable (read from `.env` / `.env.example`)
    - _Requirements: 6.1, 6.6_

  - [ ] 8.2 Implement Rider UI map and ride request flow
    - Implement Leaflet map component centred on a default coordinate pair at city-scale zoom level
    - Implement "Request Ride" button: on click, POST to `${REACT_APP_DISPATCH_URL}/request-ride` with hardcoded `rider_id` and map centre as `pickup_location`
    - On HTTP 202: display returned `trip_id` on screen
    - On non-2xx response: display human-readable error message; do NOT leave the user with a blank or crashed page (use try/catch + error state)
    - _Requirements: 6.2, 6.3, 6.4, 6.5_

  - [ ] 8.3 Write Rider UI Dockerfile
    - Write `infra/docker/rider-ui.Dockerfile` (or `services/rider-ui/Dockerfile`): multi-stage — `FROM node:20.12-alpine AS builder` runs `npm ci && npm run build`; `FROM nginx:1.25-alpine` serves static build; pinned versions, never `latest`; non-root user
    - _Requirements: 6.6, 10.7_


- [ ] 9. docker-compose local environment
  - [ ] 9.1 Write docker-compose Kafka KRaft cluster and Schema Registry
    - Define three Kafka broker services (`kafka-1`, `kafka-2`, `kafka-3`) using `confluentinc/cp-kafka:7.6.1` (pinned); configure each with `KAFKA_PROCESS_ROLES=broker,controller`, unique `KAFKA_NODE_ID`, `KAFKA_CONTROLLER_QUORUM_VOTERS` listing all three nodes, `KAFKA_LISTENERS` for both broker and controller ports; configure SASL/PLAIN authentication (`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, `KAFKA_SASL_ENABLED_MECHANISMS=PLAIN`); each service credential loaded from `${VAR_NAME}` env var substitution — no hardcoded passwords
    - Define a `kafka-init` one-shot service that creates topics `gps-pings`, `ride-events`, `dispatch-commands`, `notifications` with `replication.factor=3`, `min.insync.replicas=2` after brokers are healthy
    - Define `schema-registry` service using `confluentinc/cp-schema-registry:7.6.1` (pinned); connect to all three brokers; expose port 8081
    - Attach all Kafka services to `kafka-net` only
    - _Requirements: 7.1, 7.3, 7.9_

  - [ ] 9.2 Write docker-compose database and connection pooling services
    - Define `postgres` service using `postgres:16-alpine` (pinned): set `POSTGRES_PASSWORD` from `${POSTGRES_PASSWORD}` env var; named volume `postgres-data` for persistence; attach to `db-net` only
    - Define `pgbouncer` service using `edoburu/pgbouncer` (pinned): configure transaction pooling mode; `DB_HOST=postgres`, credentials from env vars; expose port 5432 on `db-net`; attach to `db-net` only
    - Define `redis` service using `redis:7.2-alpine` (pinned): set password via `requirepass ${REDIS_PASSWORD}`; named volume `redis-data`; attach to `db-net` only
    - Define `dynamodb-local` service using `amazon/dynamodb-local` (pinned): expose port 8000; attach to `db-net` only (Phase 2 dedup — modelled in Phase 1)
    - _Requirements: 7.1, 7.5, 7.10_

  - [ ] 9.3 Write docker-compose application service definitions
    - Define `ingest-service` container: build from Ingest Dockerfile; expose host port 8001; inject all required env vars from `${VAR_NAME}` substitution; attach to `kafka-net` and `observability-net`; health check on `GET /health`
    - Define `dispatch-service` container: build from Dispatch Dockerfile; expose host port 8080; inject all required env vars; attach to `kafka-net`, `db-net`, `frontend-net`, `observability-net`; health check on `GET /health`
    - Define `notification-service` container: build from Notification Dockerfile; expose host port 8002; inject all required env vars; attach to `kafka-net` and `observability-net`; health check on `GET /health`
    - Define `gateway-service` container: build from Gateway Dockerfile; expose host port 8003; inject all required env vars; attach to `kafka-net`, `frontend-net`, `observability-net`; health check on `GET /health`
    - Define `rider-ui` container: build from Rider UI Dockerfile; attach to `frontend-net`; inject `REACT_APP_DISPATCH_URL` build arg
    - Configure `restart: on-failure` (not `always`) so failed containers surface in `docker-compose up` output without silent infinite restart loops
    - _Requirements: 7.1, 7.2, 7.4, 7.6, 7.7, 7.8_

  - [ ] 9.4 Write docker-compose observability stack (Jaeger, Prometheus, Grafana)
    - Define `jaeger` service using `jaegertracing/all-in-one:1.56` (pinned): expose host port 16686 (UI) and OTLP receiver port 4317; attach to `observability-net`
    - Define `prometheus` service using `prom/prometheus:v2.51.0` (pinned): mount `infra/prometheus.yml` config that scrapes `/metrics` from all four application services; expose host port 9090; attach to `observability-net`
    - Define `grafana` service using `grafana/grafana:10.4.0` (pinned): expose host port 3000; configure Prometheus and Jaeger as data sources; attach to `observability-net`
    - Write `infra/prometheus.yml` with scrape configs for `ingest-service:8001`, `dispatch-service:8080`, `notification-service:8002`, `gateway-service:8003`
    - Define four named Docker networks: `kafka-net`, `db-net`, `frontend-net`, `observability-net`
    - _Requirements: 7.1, 7.8, 12.4, 12.5_


- [ ] 10. OpenAPI spec generation and commit script
  - [ ] 10.1 Implement OpenAPI spec generation and CI enforcement script
    - Verify `services/ingest/main.go` writes `services/ingest/openapi.json` at startup (auto-generated from handler registrations or a hand-authored spec matching the design)
    - Verify `services/notification/main.go` writes `services/notification/openapi.json` at startup
    - Write `scripts/generate_openapi.sh`: starts each Go service briefly (or uses a dedicated `--dump-openapi` flag), exports the spec, then stops the service; for Dispatch, calls `curl http://localhost:8080/v3/api-docs -o services/dispatch/openapi.json`
    - Add a `Makefile` target `check-openapi` that runs `generate_openapi.sh`, diffs each generated file against the committed version, and exits non-zero if any diff is found — enables CI enforcement
    - Validate all three committed `openapi.json` files are valid OpenAPI 3.x (use `swagger-cli validate` or equivalent)
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ] 11. Security baseline hardening
  - [ ] 11.1 Audit and enforce security baseline across all services and docker-compose
    - Verify `.env.example` documents every environment variable required by all services with placeholder values and inline comments explaining each variable's purpose; verify `.env` is in `.gitignore`
    - Verify `docker-compose.yml` uses `${VAR_NAME}` substitution for all credentials — no hardcoded passwords, usernames, or API keys anywhere in committed files
    - Verify Kafka SASL/PLAIN ACLs are defined in `infra/kafka/` restricting each service to only the topics it needs: `ingest` → produce `gps-pings`; `dispatch` → produce/consume `ride-events`, consume `gps-pings`; `notification` → consume `ride-events`; `gateway` → consume `ride-events`
    - Verify all four Go Dockerfiles have `USER nonroot:nonroot` and use `gcr.io/distroless/static-debian12` final stage; verify Dispatch Dockerfile has `USER appuser`; verify Rider UI Dockerfile has non-root user
    - Verify all Dockerfiles use pinned base image versions (no `latest` tags)
    - Verify `driver_id` and `rider_id` validation (non-empty, max 128 chars) is enforced in Ingest Service (`go-playground/validator` tags) and Dispatch Service (`@Valid` annotations)
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.9_

- [ ] 12. Checkpoint — All service tests pass, docker-compose starts cleanly
  - Ensure all tests in `services/ingest/tests/`, `services/dispatch/src/test/java/`, and `services/notification/tests/` pass; ensure `docker-compose up` reaches a healthy state with all services passing health checks within 120 seconds; ask the user if questions arise.

- [ ] 13. Smoke test and e2e validation
  - [ ] 13.1 Write smoke test script (`scripts/smoke_test.sh`)
    - Write `scripts/smoke_test.sh` (bash):
      1. Start Driver Simulator (`python scripts/simulate_driver.py --driver-id smoke-driver-001 --route-file scripts/sample_route.geojson --rate 10 --ingest-url http://localhost:8001`) for 5 seconds; capture PID and kill after 5 seconds
      2. Submit one Ride Request: `curl -s -X POST http://localhost:8080/request-ride -H 'Content-Type: application/json' -d '{"rider_id":"smoke-rider-001","pickup_location":{"latitude":37.7749,"longitude":-122.4194}}'`; capture `trip_id` from JSON response
      3. Poll `docker logs notification-service` every 1 second for up to 10 seconds looking for a JSON log line containing the captured `trip_id` and `"event_type":"TripAssigned"`
      4. On match found: print success message, exit 0
      5. On timeout: print diagnostic message identifying which pipeline stage did not produce output (check Ingest logs, Dispatch logs, Notification logs in sequence), exit non-zero
    - _Requirements: 11.1, 11.2, 11.4, 11.5_

  - [ ] 13.2 Write sustained throughput validation
    - Extend `scripts/smoke_test.sh` or write `scripts/throughput_test.sh`: run Driver Simulator at 10 pings/second for 5 seconds; assert zero HTTP 5xx responses from Ingest Service during the run (parse simulator stderr for error lines); exit non-zero if any 5xx is observed
    - _Requirements: 11.3_

- [ ] 14. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass across all services, `docker-compose up` starts cleanly, `scripts/smoke_test.sh` exits 0, and `make check-openapi` exits 0; ask the user if questions arise.


## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP; all property tests are optional sub-tasks
- Each task references specific requirements for traceability
- Go services use `pgregory.net/rapid` for property-based testing; Java Dispatch Service uses `jqwik`
- Each property test is tagged with `// Feature: e2e-skeleton, Property N: title` (Go) or `// Feature: e2e-skeleton, Property N: title` (Java)
- Constructor injection is used throughout Go services — no framework magic; `LocationHandler{Producer: p}`, `NewWorker(cfg, handlers)`, `WebSocketHandler{Registry: r}`
- `MaxBodySize(65536)` is a standard Go middleware `func(http.Handler) http.Handler` applied in the `chi` router chain in `main.go` — not checked inline in handlers
- `config.LoadConfig()` calls `os.Exit(1)` with a descriptive error on any missing required env var — services never start with missing configuration
- `Worker` in Notification and Gateway uses a `map[string]HandlerFunc` dispatch registry — adding a new event type handler requires no changes to the consumer loop
- All Kafka producers use `acks=all`, `enable.idempotence=true`; W3C `traceparent` headers are injected by producers and extracted by consumers
- Go Dockerfiles: `golang:1.22-alpine` build → `gcr.io/distroless/static-debian12` final; Java: `eclipse-temurin:21-jre-jammy`; Rider UI: `node:20.12-alpine` build → `nginx:1.25-alpine`
- DynamoDB Local is modelled in Phase 1 (container in docker-compose, schema defined) but the Notification Service dedup logic is activated in Phase 2
- Checkpoints at Tasks 4, 12, and 14 ensure incremental validation before proceeding

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "1.4"] },
    { "id": 2, "tasks": ["2.1", "3.1"] },
    { "id": 3, "tasks": ["2.2", "3.2", "3.3"] },
    { "id": 4, "tasks": ["2.3", "3.4", "5.1", "6.1"] },
    { "id": 5, "tasks": ["2.4", "3.5", "3.6", "5.2", "5.3", "6.2"] },
    { "id": 6, "tasks": ["2.5", "3.7", "5.4", "6.3", "6.4"] },
    { "id": 7, "tasks": ["2.6", "5.5", "6.5", "7.1", "8.1"] },
    { "id": 8, "tasks": ["2.7", "2.8", "2.9", "3.8", "3.9", "3.10", "3.11", "3.12", "5.6", "5.7", "5.8", "6.6", "7.2", "8.2"] },
    { "id": 9, "tasks": ["8.3", "9.1"] },
    { "id": 10, "tasks": ["9.2", "9.3"] },
    { "id": 11, "tasks": ["9.4"] },
    { "id": 12, "tasks": ["10.1"] },
    { "id": 13, "tasks": ["11.1"] },
    { "id": 14, "tasks": ["13.1", "13.2"] }
  ]
}
```
