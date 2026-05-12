# ADR 002: Eventual Consistency and the Outbox Pattern

**Status:** Partially Accepted — Outbox Pattern deferred to Phase 2; current skeleton operates with documented eventual consistency gaps  
**Date:** 2026-05-11  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Microservices with DDD-Influenced Bounded Contexts)

---

## Context

The platform uses an event-driven, microservices architecture where services communicate via Domain Events on Kafka. This means consistency across service boundaries is **eventual by design** — there is no distributed transaction spanning multiple services.

Two specific consistency risks exist in the current Phase 1 skeleton that must be understood and tracked:

### Risk 1: The Dual Write Problem (Dispatch Service)

The `POST /request-ride` HTTP handler does two things:
1. Publishes a `TripRequested` Domain Event to Kafka
2. Returns HTTP 202 with a `trip_id` to the caller

These two operations are not atomic. If the Kafka publish succeeds but the process crashes before returning the response, the rider has no `trip_id`. More critically, if the HTTP response is sent but the Kafka publish fails silently, the rider holds a `trip_id` that will never be assigned a driver — with no error surfaced anywhere in the system.

```
POST /request-ride
  → generate trip_id
  → publish TripRequested to Kafka   ← can fail independently
  → return HTTP 202 {trip_id}        ← can succeed even if Kafka failed
```

This is the classic dual write problem: two systems (the HTTP response and the Kafka broker) are updated without an atomic guarantee.

### Risk 2: Choreography Saga Gaps (Dispatch → Notification)

The cross-context flow is an implicit choreography-based saga:

```
TripRequested → [Dispatch consumes] → TripAssigned → [Notification consumes] → NotificationDispatched
```

There are no compensating transactions or retry orchestration for failed steps. If `TripAssigned` is published but the Notification Service crashes before processing it, the rider receives no notification. Kafka's consumer offset mechanism means the message will be redelivered on restart — but there is no timeout, no dead-letter handling, and no monitoring for trips stuck in intermediate states.

### Risk 3: GPS Ping Loss (Ingest Service)

The Ingest Service publishes `LocationPingReceived` events to Kafka and returns HTTP 202. If the Kafka publish fails after the HTTP response is sent, the ping is silently dropped. At 10 pings/second per driver this is tolerable for location tracking (the next ping arrives 100ms later), but it is still an unacknowledged loss.

---

## Decision

### Phase 1 (Current Skeleton): Accept Eventual Consistency with Documentation

For the E2E skeleton, we accept eventual consistency with the following explicit constraints:

1. **The `trip_id` returned by `POST /request-ride` is optimistic** — it is generated before the `TripRequested` event is guaranteed to be processed. The caller must not treat HTTP 202 as confirmation of driver assignment.
2. **The Dispatch Service MUST write the Trip record to PostgreSQL in the same operation as publishing to Kafka** — even without the Outbox Pattern, the Trip row must exist before the HTTP 202 is returned, so the `trip_id` is at minimum backed by a DB record.
3. **The Notification Service relies on Kafka's at-least-once delivery** — if the service restarts, uncommitted offsets will be redelivered. Notification logging must be idempotent (logging the same `TripAssigned` event twice is acceptable in the skeleton).
4. **GPS ping loss is accepted** — individual ping drops at 10/sec are tolerable. The Ingest Service logs a warning on Kafka publish failure but does not retry in Phase 1.

### Phase 2: Implement the Outbox Pattern in the Dispatch Service

The Outbox Pattern resolves the dual write problem by making the DB write and the event publish atomic:

```
POST /request-ride
  → BEGIN TRANSACTION (PostgreSQL)
      INSERT INTO trips (trip_id, rider_id, status='requested', pickup_location, requested_at)
      INSERT INTO outbox (id, event_type='TripRequested', payload='{...}', published=false)
  → COMMIT
  → return HTTP 202 {trip_id}

Outbox Relay (separate process, runs continuously):
  → SELECT * FROM outbox WHERE published = false ORDER BY created_at
  → publish each event to Kafka
  → UPDATE outbox SET published = true WHERE id = ?
```

If the transaction commits, both the Trip record and the outbox entry exist atomically. If Kafka is down, the outbox accumulates and drains when it recovers. If the relay crashes mid-publish, the unpublished entries are retried on restart (Kafka deduplication via idempotent producer handles duplicate publishes).

**Implementation options for the relay:**
- **Polling worker** (simplest): a background thread in the Dispatch Service polls the outbox table every 100–500ms
- **Debezium CDC** (production-grade): Debezium reads the PostgreSQL WAL and streams outbox entries to Kafka without polling; zero additional DB load

Phase 2 will use the polling worker approach first, with Debezium as a future upgrade path.

### Phase 2: Add Dead-Letter Handling and Saga Monitoring

To address choreography saga gaps:

1. **Dead-Letter Topic**: The Notification Service will publish unprocessable messages to a `ride-events.DLQ` topic after N failed processing attempts, rather than blocking the consumer.
2. **Trip State Timeout Monitor**: A background job in the Dispatch Service will query for trips in `requested` or `assigned` state older than a configurable threshold (default: 30 seconds) and emit a `TripStuck` alert event. This surfaces pipeline failures without requiring synchronous coordination.
3. **Idempotency Keys**: All Domain Event consumers will track processed `event_id` values in a deduplication table to handle Kafka's at-least-once redelivery safely.

---

## Consistency Model Summary

| Scope | Consistency Model | Mechanism |
|---|---|---|
| Within Dispatch Service (Trip aggregate) | Strong (ACID) | PostgreSQL transaction |
| Dispatch HTTP → Kafka publish | Eventual (Phase 1) → Atomic (Phase 2) | Outbox Pattern in Phase 2 |
| Dispatch → Notification (cross-context) | Eventual | Kafka at-least-once + consumer offset |
| Notification idempotency | At-least-once safe | Idempotent log writes (Phase 1), dedup table (Phase 2) |
| GPS ping delivery | Best-effort | 10 pings/sec; individual loss tolerated |
| Cross-context queries (Dispatch → DriverLocation) | Read-your-writes within context | Synchronous HTTP/gRPC to Location service |

---

## Known Gaps in Phase 1 (Tracked)

| Gap | Risk | Mitigation in Phase 1 | Resolution in Phase 2 |
|---|---|---|---|
| Dual write in `POST /request-ride` | `trip_id` returned with no guaranteed Kafka delivery | Trip written to DB before HTTP 202; Kafka failure logged | Outbox Pattern |
| No compensating transactions in saga | Trip stuck in `requested` with no driver assigned | Smoke test validates happy path only | Trip State Timeout Monitor |
| No dead-letter handling | Malformed `TripAssigned` event blocks Notification consumer | Notification Service skips non-`TripAssigned` events; logs warnings | DLQ topic + retry policy |
| GPS ping loss on Kafka failure | Silent location data gap | Warning logged; next ping arrives in 100ms | Outbox or retry buffer in Ingest Service |
| No idempotency enforcement | Duplicate `TripAssigned` processing on consumer restart | Duplicate log lines acceptable in skeleton | Deduplication table keyed on `event_id` |

---

## Alternatives Considered

### Synchronous Two-Phase Commit (2PC)
Rejected. Distributed 2PC across PostgreSQL and Kafka is not supported natively and introduces a coordinator that becomes a single point of failure. The operational complexity far outweighs the consistency benefit for this use case.

### Event Sourcing
Deferred (see ADR 001). Event sourcing would eliminate the dual write problem entirely — the event log is the source of truth, and the DB is a projection. However, the operational complexity (projection rebuilds, event schema evolution) is not justified for Phase 1. Revisit if the Dispatch context requires full audit/replay capability.

### Saga Orchestration (Process Manager)
Considered for Phase 2. An orchestration saga in the Dispatch Service would explicitly manage the `TripRequested → TripAssigned → NotificationDispatched` flow, with retry and compensation logic. Deferred in favour of the simpler choreography + monitoring approach. Will revisit if the saga grows beyond 3 steps.

### Kafka Transactions (Exactly-Once Semantics)
Considered for the Ingest → Kafka publish gap. Kafka's transactional producer provides exactly-once delivery within Kafka, but does not help with the HTTP response / DB write atomicity problem. Useful for the Outbox relay in Phase 2 to prevent duplicate publishes, but not a substitute for the Outbox Pattern itself.

---

## References

- [Pattern: Transactional Outbox — microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
- [Pattern: Saga — microservices.io](https://microservices.io/patterns/data/saga.html)
- [Debezium — CDC for PostgreSQL](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Kafka Transactions — Confluent](https://www.confluent.io/blog/transactions-apache-kafka/)
