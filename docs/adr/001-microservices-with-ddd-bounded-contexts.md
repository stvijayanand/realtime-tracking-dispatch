# ADR 001: Microservices Architecture with DDD-Influenced Bounded Contexts

**Status:** Accepted — partially amended by ADR 005 (CQRS local read model for Dispatch)  
**Date:** 2026-05-11  
**Deciders:** Platform Engineering Team

---

## Context

The platform needs to handle millions of concurrent GPS ping producers, real-time driver-to-rider matching, live ETA streaming, and push notifications. We are building this as a distributed system from day one.

Two architectural patterns were considered:

- **Pure Microservices** — services decomposed by technical capability (ingest, process, notify), with shared DTOs in a common library
- **Microservices with DDD-Influenced Bounded Contexts** — services decomposed by domain concept, each owning its model, with domain events as the integration contract

The initial scaffold decomposed services by technical function (`ingest`, `dispatch`, `notification`, `tracking`, `gateway`). This is close to DDD but lacks explicit domain modelling — no aggregate roots, no typed domain events, and a `shared/` library that risks becoming a coupling point.

---

## Decision

We adopt **Microservices with DDD-Influenced Bounded Contexts**.

Each service is treated as a Bounded Context:

| Bounded Context | Service(s) | Core Aggregate | Owns |
|---|---|---|---|
| **Location** | `ingest`, `tracking` | `DriverLocation` | GPS pings, geospatial state, ETA computation |
| **Dispatch** | `dispatch` | `Trip` | Trip lifecycle, driver assignment, matching logic |
| **Notification** | `notification` | `Notification` | Delivery state, provider abstraction |
| **Client Gateway** | `gateway` | — | Auth, WebSocket/SSE fan-out, API proxying |

### Domain Events (Kafka integration contract)

Services integrate exclusively via typed Domain Events published to Kafka. Event names use past-tense domain language:

| Event | Producer | Consumer(s) | Topic |
|---|---|---|---|
| `LocationPingReceived` | Location (ingest) | Location (tracking) — authoritative; Dispatch — local read model | `gps-pings` |
| `TripRequested` | Dispatch | Dispatch (self, async) | `ride-events` |
| `TripAssigned` | Dispatch | Notification | `ride-events` |
| `TripCompleted` | Dispatch | Notification | `ride-events` |
| `NotificationDispatched` | Notification | — | `notifications` |

See ADR 005 for why Dispatch consumes `LocationPingReceived` events.

### Aggregate Ownership

- **`Trip`** is owned exclusively by the Dispatch bounded context. No other service reads or writes the Trip table directly.
- **`DriverLocation`** is owned by the Location bounded context (Tracking service). Dispatch does **not** query Location synchronously — see ADR 005. Instead, Dispatch maintains a local read model (CQRS projection) by consuming `LocationPingReceived` events, which it uses for nearest-driver matching without any cross-service call.
- **`Notification`** is owned by the Notification bounded context. It receives domain events and manages its own delivery state.

### `shared/` Scope Restriction

The `shared/` directory is restricted to **infrastructure concerns only**:
- Kafka message envelope schema (headers, metadata)
- Health check response DTOs
- Common error envelope shapes
- Protobuf/Avro schema definitions (if schema registry is adopted)

Domain objects (Trip, Driver, Rider, Location) are **never** placed in `shared/`. Each bounded context defines its own internal model.

---

## Consequences

### Positive

- Service boundaries are stable — they reflect the domain, not the current technical decomposition
- Domain Events as the integration contract means services are decoupled at the schema level, not just the network level
- The `Trip` aggregate owns its state machine, making lifecycle transitions explicit and auditable
- Ubiquitous Language in code: class names, method names, and event names match the domain glossary

### Negative / Trade-offs

- More upfront modelling required — can't just add a field to a shared DTO
- Each bounded context may duplicate some types (e.g., both Dispatch and Notification have a concept of `trip_id`) — this is intentional, not a bug
- ~~Synchronous cross-context queries (e.g., Dispatch querying DriverLocation) require an internal API contract, adding a network hop~~ — **Retired by ADR 005**: Dispatch uses a CQRS local read model instead of synchronous cross-context queries

### Neutral

- The current service names (`ingest`, `dispatch`, `notification`, `tracking`, `gateway`) are preserved — they already map well to bounded contexts
- Kafka topic names (`gps-pings`, `ride-events`, `notifications`) are preserved; message payloads gain an explicit `event_type` field to distinguish domain events on the same topic

---

## Alternatives Considered

### Pure Microservices with Shared DTOs
Rejected. The `shared/` library becomes a coupling point — a change to a shared DTO requires coordinated deployment of all consumers. This defeats the purpose of independent deployability.

### Monolith First
Rejected. The scale requirements (millions of GPS producers, real-time matching) make a monolith impractical from the start. The team has the capability to operate microservices.

### Full DDD with CQRS and Event Sourcing
CQRS is now **partially adopted** — the Dispatch service uses a CQRS local read model for driver location (see ADR 005). Full event sourcing remains deferred. Event sourcing adds significant operational complexity (event store, projection rebuilds). We adopt DDD tactical patterns (aggregates, domain events, bounded contexts, CQRS projections) without event sourcing for now.

---

## References

- [Domain-Driven Design — Eric Evans](https://www.domainlanguage.com/ddd/)
- [Building Microservices — Sam Newman](https://samnewman.io/books/building_microservices/)
- [Implementing Domain-Driven Design — Vaughn Vernon](https://vaughnvernon.com/?page_id=168)
