# ADR 005: CQRS with Local Read Model for Dispatch — Retiring Synchronous Cross-Context Queries

**Status:** Accepted  
**Date:** 2026-05-11  
**Deciders:** Platform Engineering Team  
**Supersedes:** ADR 001 (partially) — the "Aggregate Ownership" section's statement that "Dispatch queries DriverLocation via a synchronous internal API" is retired by this decision  
**Relates to:** ADR 001 (Bounded Contexts), ADR 002 (Eventual Consistency)

---

## Context

ADR 001 established that cross-context queries go through a synchronous internal HTTP/gRPC API. Specifically, the Dispatch service was expected to query the Location bounded context to find nearby drivers when processing a `TripRequested` event.

This creates a direct synchronous dependency between Dispatch and Location in the hot path:

```
TripRequested consumed
  → Dispatch calls Location service HTTP/gRPC: "give me drivers near (lat, lng)"
  → Location queries Redis GEORADIUS
  → returns driver list
  → Dispatch selects nearest driver
  → publishes TripAssigned
```

**Problems with this pattern:**

1. **Cascading failure**: If the Location/Tracking service is slow or unavailable, every ride request fails or degrades. Dispatch's SLA is now coupled to Location's SLA.
2. **Latency amplification**: A synchronous network hop to another service adds latency on every dispatch decision. At scale (thousands of concurrent ride requests), this compounds.
3. **Tight deployment coupling**: Dispatch cannot be deployed or scaled independently without considering the Location service's availability.
4. **Contradicts the bounded context principle**: Requiring Dispatch to call Location synchronously means Dispatch cannot function autonomously — it is not truly independent.

The `LocationPingReceived` event is already being published to the `gps-pings` Kafka topic at 10 pings/second per driver. This data is available asynchronously. There is no fundamental reason Dispatch needs to query it synchronously.

---

## Decision

We adopt **CQRS (Command Query Responsibility Segregation) with a local read model** for the Dispatch bounded context.

### What changes

The Dispatch service maintains its own **local read model** of driver locations, kept up to date by consuming `LocationPingReceived` events from the `gps-pings` Kafka topic. When a `TripRequested` event is processed, Dispatch queries its own local Redis geospatial index — no cross-service call is made.

```
[Write side — Location BC]
Ingest Service → publishes LocationPingReceived → gps-pings topic

[Read model update — Dispatch BC]
Dispatch consumer (gps-pings) → GEOADD dispatch:drivers {lat} {lng} {driver_id} in Dispatch's Redis namespace

[Query — Dispatch BC, fully local]
TripRequested consumed
  → Dispatch queries its own Redis: GEORADIUS dispatch:drivers {lat} {lng} 5 km ASC COUNT 1
  → selects nearest available driver
  → publishes TripAssigned
```

### Consumer group separation

The Dispatch service uses a **separate consumer group** for `gps-pings` from the Tracking service's consumer group. Both consume the same events independently:

| Consumer | Topic | Consumer Group | Purpose |
|---|---|---|---|
| `tracking` | `gps-pings` | `tracking-service-group` | Authoritative geospatial state, ETA computation, rider-facing streams |
| `dispatch` | `gps-pings` | `dispatch-location-group` | Local read model for nearest-driver matching only |

### Redis namespace separation

Dispatch's local read model lives in a **separate Redis key namespace** from the Location/Tracking service's authoritative state:

- Tracking service: `location:drivers:{driver_id}` — authoritative, full history
- Dispatch service: `dispatch:drivers` (ZSET, geospatial) — read model, matching only

These are logically separate even if they share the same Redis instance in Phase 1. In Phase 2+, they can be split into separate Redis instances per bounded context.

### Consistency model

The Dispatch local read model is **eventually consistent** with the authoritative Location state. At 10 pings/second per driver, the maximum staleness is bounded by Kafka consumer lag — typically under 100ms in a healthy system. This is acceptable for driver matching: a 100ms-stale location does not meaningfully affect which driver is nearest.

This is documented as an accepted trade-off in ADR 002 (Eventual Consistency).

### What the Tracking service still owns

The Tracking service remains the **authoritative owner** of `DriverLocation`. It:
- Maintains the canonical geospatial index used for ETA computation
- Streams live ETAs to riders via WebSocket/SSE
- Is the source of truth for driver location history and analytics

Dispatch's local read model is a **projection** of Location events, not a replacement for the Tracking service.

### Phase 1 stub behaviour

In the Phase 1 skeleton, the Dispatch service's `gps-pings` consumer is implemented as a **stub**:
- It consumes `LocationPingReceived` events and logs receipt to stdout
- It does NOT yet update a Redis geospatial index (Redis is available in the compose environment but the GEOADD logic is deferred)
- The hardcoded nearest-driver logic (static in-memory list) continues to be used for matching in Phase 1
- The stub establishes the consumer group, validates the event envelope, and proves the data flow

This means Phase 2 can add the Redis GEOADD logic to an already-wired consumer without any architectural change.

---

## Updated Domain Events Table

| Event | Producer | Consumer(s) | Topic | Consumer Purpose |
|---|---|---|---|---|
| `LocationPingReceived` | `ingest` | `tracking` (authoritative), `dispatch` (read model) | `gps-pings` | Tracking: geospatial state + ETAs; Dispatch: local matching index |
| `TripRequested` | `dispatch` | `dispatch` (self, async) | `ride-events` | Dispatch: trigger matching |
| `TripAssigned` | `dispatch` | `notification` | `ride-events` | Notification: log/send notification |
| `TripCompleted` | `dispatch` | `notification` | `ride-events` | Notification: log/send notification |
| `NotificationDispatched` | `notification` | — | `notifications` | — |

---

## Consequences

### Positive

- Dispatch is fully autonomous — it can process ride requests even if the Location/Tracking service is down
- No synchronous cross-context dependency in the hot path — eliminates cascading failure risk
- Dispatch latency is bounded by local Redis query time (~1ms), not a network hop to another service
- Aligns with how high-throughput dispatch systems (Uber, Lyft) actually work in production
- The `gps-pings` consumer in Dispatch is a natural extension — the data is already flowing

### Negative / Trade-offs

- Dispatch now consumes two Kafka topics (`gps-pings` and `ride-events`) — slightly more complex consumer setup
- The local read model is eventually consistent — Dispatch may match on a location that is up to ~100ms stale
- Driver location data is replicated across two Redis namespaces — intentional denormalization, not a bug
- If a driver goes offline, Dispatch's local read model will not know until the next ping fails to arrive — requires a TTL-based eviction strategy (Phase 2)

### Retired

- The statement in ADR 001 that "Dispatch queries DriverLocation via a synchronous internal API (gRPC or HTTP)" is **retired**. No synchronous cross-context query exists in the hot path.
- The "Negative / Trade-offs" item in ADR 001 — "Synchronous cross-context queries require an internal API contract, adding a network hop" — is resolved by this decision.

---

## Alternatives Considered

### Synchronous query with circuit breaker (Option B)
Rejected. A circuit breaker mitigates the failure mode but does not eliminate the latency amplification or the deployment coupling. The fallback logic (what does Dispatch do when the circuit is open?) is complex and error-prone. The local read model is strictly better.

### Hybrid: local replica for matching, synchronous confirmation before TripAssigned (Option C)
Rejected for Phase 1. The confirmation step adds latency and reintroduces the synchronous dependency. The eventual consistency of the local read model is acceptable for matching — a 100ms-stale location does not change the dispatch outcome in practice. Revisit if regulatory or contractual requirements demand authoritative location confirmation.

### Shared Redis instance with shared key namespace
Rejected. Sharing a Redis key namespace between Tracking and Dispatch creates implicit coupling — a schema change in Tracking's key structure breaks Dispatch. Separate namespaces (even on the same Redis instance) maintain bounded context isolation.

---

## References

- [CQRS Pattern — Martin Fowler](https://martinfowler.com/bliki/CQRS.html)
- [Read Model / Projection Pattern — microservices.io](https://microservices.io/patterns/data/cqrs.html)
- [How Uber Manages Driver Location at Scale](https://www.uber.com/en-US/blog/engineering/)
- ADR 001: Microservices with DDD-Influenced Bounded Contexts
- ADR 002: Eventual Consistency and the Outbox Pattern
