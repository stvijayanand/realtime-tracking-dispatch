# Project Structure

> This project is greenfield. The structure below is the target layout for a microservices-based dispatch platform using DDD-influenced bounded contexts (see ADR 001). Update as services are scaffolded.

## Top-Level Layout

```
realtime-tracking-dispatch/
├── services/                  # Individual microservices (one per bounded context)
│   ├── ingest/                # Location BC — GPS ping ingestion (Go)
│   ├── dispatch/              # Dispatch BC — Trip aggregate, driver matching (Java 21)
│   ├── tracking/              # Location BC — ETA computation, geospatial state (Go)
│   ├── notification/          # Notification BC — push notification delivery (Go)
│   └── gateway/               # Client Gateway BC — WebSocket/SSE hub, Kafka→WS bridge (Go)
├── shared/                    # Infrastructure concerns ONLY (see Conventions)
│   ├── proto/                 # Protobuf definitions (if using gRPC for internal APIs)
│   ├── avro/                  # Avro schemas for all Kafka Domain Events (Schema Registry)
│   └── envelope/              # Kafka envelope types (Go + Java)
├── infra/                     # Infrastructure-as-code
│   ├── docker/                # Dockerfiles per service (multi-stage, distroless for Go)
│   ├── k8s/                   # Kubernetes manifests (Deployments, Services, HPAs, KEDA ScaledObjects)
│   │   └── kafka/             # Strimzi CRDs: Kafka cluster, KafkaTopic, KafkaUser (per-service ACLs)
│   ├── kafka/                 # Topic configs, Schema Registry config, ACL definitions
│   └── terraform/             # AWS infrastructure (EKS, Aurora Serverless v2, ElastiCache, ECR, auto-destroy Lambda)
├── scripts/                   # Dev/ops helper scripts
├── docs/                      # Architecture diagrams, ADRs
│   ├── adr/                   # Architecture Decision Records (NNN-title.md)
│   └── query-plans/           # EXPLAIN ANALYZE output for all PostgreSQL queries
├── .kiro/                     # Kiro specs and steering
│   ├── specs/                 # Feature specs (requirements, design, tasks)
│   └── steering/              # AI steering rules (this folder)
├── docker-compose.yml         # Local development environment (full stack)
├── Requirements.md            # High-level product requirements
└── README.md
```

## Bounded Contexts → Services

| Bounded Context | Service(s) | Language | Core Aggregate | Owns |
|---|---|---|---|---|
| **Location** | `ingest`, `tracking` | Go | `DriverLocation` | GPS pings, geospatial state (Redis GEOADD), ETA computation |
| **Dispatch** | `dispatch` | Java 21 | `Trip` | Trip lifecycle, driver assignment, matching logic, PostgreSQL Trip table |
| **Notification** | `notification` | Go | `Notification` | Push notification delivery (FCM/APNs), DynamoDB dedup table |
| **Client Gateway** | `gateway` | Go | — | WebSocket/SSE hub (Kafka consumer → WebSocket bridge), session registry, API proxying |

## Domain Events (Kafka Integration Contract)

Services integrate via typed **Avro-serialised** Domain Events (Schema Registry enforces compatibility). Event names use past-tense domain language:

| Event | Producer | Consumer(s) | Topic | Consumer Purpose |
|---|---|---|---|---|
| `LocationPingReceived` | `ingest` | `tracking` (authoritative), `dispatch` (read model) | `gps-pings` | Tracking: geospatial state + ETAs; Dispatch: local Redis GEOADD index |
| `TripRequested` | `dispatch` | `dispatch` (self, async) | `ride-events` | Dispatch: trigger matching |
| `TripAssigned` | `dispatch` | `notification`, `gateway` | `ride-events` | Notification: FCM push (best-effort); Gateway: WebSocket push to connected rider |
| `TripCancelled` | `dispatch` | `notification`, `gateway`, `tracking` | `ride-events` | Notification: push cancellation; Gateway: update rider UI; Tracking: stop ETA streaming |
| `TripExpired` | `dispatch` | `notification`, `gateway` | `ride-events` | Notification: push expiry; Gateway: update rider UI |
| `TripCompleted` | `dispatch` | `notification`, `gateway` | `ride-events` | Notification: push completion; Gateway: update rider UI |
| `ETAUpdated` | `tracking` | `gateway` | `ride-events` | Gateway: stream live ETA to connected rider via WebSocket |
| `NotificationDispatched` | `notification` | — | `notifications` | Audit trail only |

**Consumer groups**: `dispatch-consumer-group`, `notification-consumer-group`, `gateway-consumer-group`, `dispatch-location-group` (gps-pings CQRS stub). Each is independent — Kafka fans out to all groups simultaneously.

All Kafka messages use Avro schema registered in Schema Registry. The `event_type` field in the envelope distinguishes domain events on the same topic.

## Conventions

- Each service is independently deployable with its own `Dockerfile` (multi-stage, distroless for Go) and config
- Each bounded context defines its own internal domain model — domain objects are **never** placed in `shared/`
- `shared/` is restricted to infrastructure concerns: Avro schemas (Schema Registry), Kafka envelope types, health check DTOs, common error shapes, proto definitions
- It is acceptable (and expected) for multiple bounded contexts to have their own representation of concepts like `trip_id` — this is intentional decoupling, not duplication
- All inter-service communication goes through Kafka Domain Events (async) — there are NO synchronous cross-context queries in the hot path
- **Kafka fans out events to consumer groups** — the Gateway Service is a Kafka consumer (`gateway-consumer-group`); it translates Kafka events to WebSocket frames; it does not fan out events
- The Dispatch service maintains a local CQRS read model of driver locations (Redis GEOADD) by consuming `LocationPingReceived` events — it never calls the Location/Tracking service synchronously (see ADR 005)
- Synchronous HTTP/gRPC between services is only permitted for non-hot-path operations (admin queries, health checks)
- Environment-specific config via environment variables — no hardcoded secrets or URLs
- Production secrets via HashiCorp Vault with Vault Agent sidecar or Vault SDK (AppRole / Kubernetes auth method)
- Each service owns its own database schema; no cross-service direct DB access
- PostgreSQL queries must have `EXPLAIN ANALYZE` output committed to `docs/query-plans/`
- DynamoDB is used for high-write-throughput, single-key-lookup tables (notification dedup, idempotency keys) — not for relational data
- ADRs go in `docs/adr/` with format `NNN-title.md`
- Kafka consumer pods scale via KEDA `ScaledObject` based on consumer group lag — not CPU
