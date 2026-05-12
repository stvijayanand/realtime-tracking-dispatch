# Project Structure

> This project is greenfield. The structure below is the target layout for a microservices-based dispatch platform using DDD-influenced bounded contexts (see ADR 001). Update as services are scaffolded.

## Top-Level Layout

```
realtime-tracking-dispatch/
├── services/                  # Individual microservices (one per bounded context)
│   ├── ingest/                # Location BC — GPS ping ingestion
│   ├── dispatch/              # Dispatch BC — Trip aggregate, driver matching
│   ├── tracking/              # Location BC — ETA computation, geospatial state
│   ├── notification/          # Notification BC — push notification delivery
│   └── gateway/               # Client Gateway BC — auth, WebSocket/SSE hub
├── shared/                    # Infrastructure concerns ONLY (see Conventions)
├── infra/                     # Infrastructure-as-code
│   ├── docker/                # Dockerfiles per service
│   ├── k8s/                   # Kubernetes manifests
│   └── kafka/                 # Topic configs, schema registry
├── scripts/                   # Dev/ops helper scripts
├── docs/                      # Architecture diagrams, ADRs
│   └── adr/                   # Architecture Decision Records (NNN-title.md)
├── .kiro/                     # Kiro specs and steering
│   ├── specs/                 # Feature specs (requirements, design, tasks)
│   └── steering/              # AI steering rules (this folder)
├── docker-compose.yml         # Local development environment
├── Requirements.md            # High-level product requirements
└── README.md
```

## Bounded Contexts → Services

| Bounded Context | Service(s) | Core Aggregate | Owns |
|---|---|---|---|
| **Location** | `ingest`, `tracking` | `DriverLocation` | GPS pings, geospatial state, ETA computation |
| **Dispatch** | `dispatch` | `Trip` | Trip lifecycle, driver assignment, matching logic |
| **Notification** | `notification` | `Notification` | Delivery state, provider abstraction |
| **Client Gateway** | `gateway` | — | Auth, WebSocket/SSE fan-out, API proxying |

## Domain Events (Kafka Integration Contract)

Services integrate via typed Domain Events. Event names use past-tense domain language:

| Event | Producer | Consumer(s) | Topic | Consumer Purpose |
|---|---|---|---|---|
| `LocationPingReceived` | `ingest` | `tracking` (authoritative), `dispatch` (read model) | `gps-pings` | Tracking: geospatial state + ETAs; Dispatch: local matching index |
| `TripRequested` | `dispatch` | `dispatch` (self, async) | `ride-events` | Dispatch: trigger matching |
| `TripAssigned` | `dispatch` | `notification` | `ride-events` | Notification: log/send notification |
| `TripCompleted` | `dispatch` | `notification` | `ride-events` | Notification: log/send notification |
| `NotificationDispatched` | `notification` | — | `notifications` | — |

All Kafka messages include an `event_type` field in the payload to distinguish domain events on the same topic.

## Conventions

- Each service is independently deployable with its own `Dockerfile` and config
- Each bounded context defines its own internal domain model — domain objects are **never** placed in `shared/`
- `shared/` is restricted to infrastructure concerns: Kafka envelope schema, health check DTOs, common error shapes, proto/Avro definitions
- It is acceptable (and expected) for multiple bounded contexts to have their own representation of concepts like `trip_id` — this is intentional decoupling, not duplication
- All inter-service communication goes through Kafka Domain Events (async) — there are NO synchronous cross-context queries in the hot path
- The Dispatch service maintains a local CQRS read model of driver locations by consuming `LocationPingReceived` events — it never calls the Location/Tracking service synchronously (see ADR 005)
- Synchronous HTTP/gRPC between services is only permitted for non-hot-path operations (e.g., admin queries, health checks)
- Environment-specific config via environment variables — no hardcoded secrets or URLs
- Each service owns its own database schema; no cross-service direct DB access
- ADRs go in `docs/adr/` with format `NNN-title.md`
