# ADR 006: Saga Pattern — Choreography (Phase 1/2) to Orchestration (Phase 3)

**Status:** Accepted  
**Date:** 2026-05-11  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Bounded Contexts), ADR 002 (Eventual Consistency), ADR 005 (CQRS)

---

## Context

The platform manages distributed transactions that span multiple bounded contexts — a ride request must coordinate driver matching, trip assignment, ETA streaming, payment capture, and push notifications. No single database transaction can span these services. The Saga pattern is the standard approach for managing such distributed transactions with compensating actions on failure.

There are two variants:

- **Choreography**: Each service reacts to domain events and emits the next event. No central coordinator. Services are fully decoupled.
- **Orchestration**: A central Saga Orchestrator (process manager) explicitly drives each step, tracks state, issues commands, and executes compensating actions on failure.

The right choice depends on the complexity of the saga — specifically the number of steps, the degree of branching, and how many bounded contexts are involved.

### Phase 1 saga (current skeleton)

```
TripRequested → TripAssigned → NotificationDispatched
```

3 steps, 2 bounded contexts (Dispatch, Notification), no branching, no compensating actions needed in the happy path.

### Full system saga (Phase 3+)

```
TripRequested
  → [Dispatch] no driver available? → TripExpired → compensate: notify rider
  → [Dispatch] driver matched → TripAssigned
      → [Driver app] driver declines → re-match (up to N attempts) or TripExpired
      → [Driver app] driver accepts → TripAccepted
          → [Tracking] driver en route to pickup → ETAUpdated (streaming)
          → [Tracking] driver arrived → DriverArrived → notify rider
          → [Dispatch] rider picked up → TripStarted
              → [Tracking] en route to destination → ETAUpdated (streaming)
              → [Dispatch] destination reached → TripCompleted
                  → [Payment] capture payment → PaymentCaptured
                      → payment fails → PaymentFailed → compensate: retry or flag for manual review
                  → [Notification] notify rider + driver → NotificationDispatched
                  → [Rating] prompt for rating → RatingRequested
```

8–10 steps, 5 bounded contexts (Dispatch, Tracking, Notification, Payment, Rating), multiple branching points, compensating actions at each failure point.

---

## Decision

### Phase 1 (Skeleton): Implicit Choreography

The Phase 1 saga is 3 steps and linear. Choreography is sufficient. No formal saga infrastructure is needed.

The only Phase 1 requirement is that the `Trip` aggregate in the Dispatch service tracks its own state — this is local state management, not distributed coordination.

### Phase 2 (Real Matching): Formal Choreography + Saga State Monitor

As driver acceptance/rejection and ETA streaming are added, the saga grows to 5–6 steps. Choreography remains the right pattern, but requires:

1. **Explicit Trip state machine** in the Dispatch service (see below)
2. **Compensating domain events** modelled as first-class events (`TripCancelled`, `TripExpired`, `TripNoDriverAvailable`)
3. **Saga State Monitor** — a background job in Dispatch that queries for trips stuck in intermediate states beyond a timeout threshold and emits compensating events
4. **Dead-letter handling** for failed event processing (see ADR 002)

### Phase 3 (Full Lifecycle): Orchestration via Process Manager in Dispatch

**Trigger conditions for migration to orchestration** (any one of these):
- The saga exceeds 5 steps
- The saga involves more than 3 bounded contexts outside Dispatch
- A compensating action in one context requires coordinating responses from multiple other contexts simultaneously
- Debugging a stuck saga requires correlating logs across more than 3 services

At Phase 3, a **Saga Orchestrator** (process manager) is introduced inside the Dispatch bounded context. It:
- Owns the full Trip state machine
- Issues explicit commands to other services (not just emitting events)
- Waits for reply events with timeouts
- Executes compensating actions when steps fail or time out
- Persists saga state to PostgreSQL so it survives restarts

The orchestrator lives inside Dispatch because Dispatch owns the `Trip` aggregate — it is the natural home for trip lifecycle coordination. It does not become a separate service.

---

## Trip State Machine

The `Trip` aggregate maintains an explicit state machine. All state transitions are persisted to PostgreSQL and emitted as domain events.

```
                    ┌─────────────────────────────────────────────┐
                    │                                             │
         ┌──────────▼──────────┐                                 │
         │     REQUESTED       │ ──── no driver / timeout ──────►│
         └──────────┬──────────┘                                 │
                    │ driver matched                              │
         ┌──────────▼──────────┐                                 │
         │     ASSIGNING       │ ──── driver declines (N times) ►│
         └──────────┬──────────┘                                 │
                    │ driver accepts                              │
         ┌──────────▼──────────┐                                 │
         │     ASSIGNED        │                                 │
         └──────────┬──────────┘                                 │
                    │ driver en route                             │
         ┌──────────▼──────────┐                                 │
         │    EN_ROUTE_PICKUP  │ ──── timeout ──────────────────►│
         └──────────┬──────────┘                                 │
                    │ driver arrived                              │
         ┌──────────▼──────────┐                                 │
         │  DRIVER_ARRIVED     │                                 │
         └──────────┬──────────┘                                 │
                    │ rider picked up                             │
         ┌──────────▼──────────┐                                 │
         │      IN_PROGRESS    │ ──── timeout ──────────────────►│
         └──────────┬──────────┘                                 │
                    │ destination reached                         │
         ┌──────────▼──────────┐                                 │
         │     COMPLETING      │ ──── payment failure ──────────►│
         └──────────┬──────────┘                                 │
                    │ payment captured                            │
         ┌──────────▼──────────┐    ┌──────────────────────────┐│
         │     COMPLETED       │    │        CANCELLED         ◄┘│
         └─────────────────────┘    └──────────────────────────┘
```

### Phase 1 states (skeleton)

Only three states are implemented in Phase 1:

| State | Entered when | Domain Event emitted |
|---|---|---|
| `REQUESTED` | `POST /request-ride` received | `TripRequested` |
| `ASSIGNED` | Driver selected from static list | `TripAssigned` |
| `CANCELLED` | (not triggered in Phase 1 — state exists in model only) | `TripCancelled` |

### Full state machine (Phase 2/3)

| State | Entered when | Domain Event emitted | Compensating event |
|---|---|---|---|
| `REQUESTED` | Ride request received | `TripRequested` | — |
| `ASSIGNING` | Matching in progress | — | `TripExpired` (timeout) |
| `ASSIGNED` | Driver matched | `TripAssigned` | `TripCancelled` (driver declines N times) |
| `ACCEPTED` | Driver accepts | `TripAccepted` | `TripCancelled` (driver no-show timeout) |
| `EN_ROUTE_PICKUP` | Driver starts driving to pickup | `DriverEnRoute` | `TripCancelled` (timeout) |
| `DRIVER_ARRIVED` | Driver at pickup location | `DriverArrived` | — |
| `IN_PROGRESS` | Rider picked up | `TripStarted` | — |
| `COMPLETING` | Destination reached, payment processing | `TripCompleted` | `PaymentFailed` |
| `COMPLETED` | Payment captured | `PaymentCaptured` | — |
| `CANCELLED` | Any compensating action | `TripCancelled` | — |
| `EXPIRED` | No driver found within timeout | `TripExpired` | — |

---

## Rider Awareness: Push Notification vs. Real-Time State Stream

Push notification (FCM/APNs) and rider awareness are **not the same thing**. They are two independent channels with different reliability characteristics:

| Channel | Mechanism | Reliability | Use case |
|---|---|---|---|
| **WebSocket/SSE stream** (Gateway) | Gateway consumes `TripAssigned` from Kafka and pushes state update to connected rider client | Reliable — primary channel for connected clients | Rider app is open/foreground |
| **Push notification** (Notification Service) | FCM/APNs delivery to device | Best-effort — secondary channel | Rider app is backgrounded or device is offline |
| **Polling fallback** | Rider UI polls `GET /trips/{trip_id}` | Always available — tertiary fallback | WebSocket dropped, push missed |

When `TripAssigned` is published:
1. The **Gateway** consumes it and immediately pushes the state update to any connected rider WebSocket/SSE session — this is the primary real-time delivery mechanism
2. The **Notification Service** sends a FCM/APNs push notification — this is for backgrounded/offline clients
3. The **Trip state in PostgreSQL** is updated to `ASSIGNED` — the rider can poll for this at any time

A push notification failure (`NotificationFailed`) does **not** mean the rider is unaware. The Gateway WebSocket stream is the authoritative real-time channel. Push is a supplementary delivery mechanism for clients that are not actively connected.

This means:
- `NotificationFailed` → retry via DLQ, do not block trip progression, rider is informed via WebSocket regardless
- If the rider's WebSocket session is also disconnected, the polling fallback (`GET /trips/{trip_id}`) returns the current `ASSIGNED` state
- The rider is only truly unaware if all three channels fail simultaneously — which requires the Gateway, the Notification Service, and the Dispatch HTTP API to all be unavailable at the same time



Each failure point has an explicit compensating action:

| Failure | Compensating Event | Effect |
|---|---|---|
| No driver available after N attempts | `TripExpired` | Notify rider; release any held resources |
| Driver declines N times | `TripCancelled` | Re-queue or notify rider of cancellation |
| Driver no-show (timeout at pickup) | `TripCancelled` | Notify rider; mark driver as unavailable |
| Payment capture fails | `PaymentFailed` | Retry up to 3 times; flag for manual review if all retries fail |
| Notification delivery fails | `NotificationFailed` | Retry up to 3 times via DLQ; rider is informed via Gateway WebSocket/SSE stream regardless of push outcome; trip progression is not blocked (see "Rider Awareness" section above) |

Compensating events are consumed by the relevant bounded contexts:
- `TripCancelled` / `TripExpired` → Notification (notify rider and driver), Tracking (stop ETA streaming)
- `PaymentFailed` → Notification (notify rider), Payment (retry logic)

---

## Saga State Monitor (Phase 2)

A background job in the Dispatch service runs every 10 seconds and queries for trips in intermediate states beyond their timeout threshold:

```sql
SELECT trip_id, status, updated_at
FROM trips
WHERE status IN ('REQUESTED', 'ASSIGNING', 'ASSIGNED', 'EN_ROUTE_PICKUP', 'IN_PROGRESS')
  AND updated_at < NOW() - INTERVAL '${timeout_seconds} seconds'
```

For each stuck trip, the monitor emits the appropriate compensating event and transitions the trip to `CANCELLED` or `EXPIRED`.

**Timeout thresholds (configurable via environment variables):**

| State | Default timeout |
|---|---|
| `REQUESTED` / `ASSIGNING` | 30 seconds |
| `ASSIGNED` (awaiting acceptance) | 60 seconds |
| `EN_ROUTE_PICKUP` | 30 minutes |
| `IN_PROGRESS` | 4 hours |

---

## Phase Migration Checklist

### Phase 1 → Phase 2 (add formal choreography)
- [ ] Implement full Trip state machine in Dispatch (`trips` table with `status` column)
- [ ] Add `TripCancelled` and `TripExpired` domain events
- [ ] Implement Saga State Monitor background job
- [ ] Add driver acceptance/rejection flow
- [ ] Add DLQ for failed event processing (see ADR 002)

### Phase 2 → Phase 3 (migrate to orchestration)
Trigger: saga exceeds 5 steps OR involves 3+ bounded contexts outside Dispatch

- [ ] Design Saga Orchestrator state machine (extends Trip state machine)
- [ ] Replace event-reaction pattern with explicit command/reply pattern for cross-context steps
- [ ] Add saga persistence table (`saga_state`) separate from `trips`
- [ ] Implement timeout and retry logic in orchestrator
- [ ] Add distributed tracing (correlation ID propagated through all saga steps)

---

## Consequences

### Positive
- Phase 1/2 choreography keeps services decoupled and the hot path fast
- Explicit Trip state machine gives full visibility into saga state from day one
- Clear trigger conditions prevent premature migration to orchestration
- Compensating events are first-class domain events — they appear in the event log and can be replayed

### Negative / Trade-offs
- Phase 2 Saga State Monitor adds a polling query to PostgreSQL — acceptable at this scale, revisit at high trip volume
- Phase 3 orchestration migration requires significant refactoring of cross-context event flows
- The orchestrator inside Dispatch creates a logical coupling between Dispatch and the full trip lifecycle — this is intentional (Dispatch owns Trip) but must be managed carefully

---

## References

- [Saga Pattern — microservices.io](https://microservices.io/patterns/data/saga.html)
- [Choreography vs Orchestration — Camunda](https://camunda.com/blog/2021/02/orchestration-vs-choreography/)
- [Process Manager Pattern — Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html)
- [Compensating Transaction Pattern — Microsoft](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction)
