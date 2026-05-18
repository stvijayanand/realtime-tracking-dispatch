# ADR 004: Security Model

**Status:** Partially Accepted — Phase 1 controls implemented in skeleton; Phase 2+ controls designed and deferred  
**Date:** 2026-05-11  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Bounded Contexts), ADR 002 (Eventual Consistency), ADR 003 (Idempotency)

---

## Context

The platform handles sensitive data: real-time driver locations, rider identities, and trip records. Without security controls from day one, the skeleton establishes patterns that get copy-pasted into production. The goal is to bake in the right foundations in Phase 1 — even where full enforcement is deferred — so that security is never retrofitted.

Security is addressed across five layers:

1. **Secrets Management** — how credentials are stored and injected
2. **Network Isolation** — which containers can reach which
3. **Kafka Authorization** — which services can produce/consume which topics
4. **Transport Security** — encryption in transit
5. **Application-Layer Security** — authentication, authorization, input validation, rate limiting

---

## Decision

### Layer 1: Secrets Management (Phase 1)

**No secrets in source control. Ever.**

All credentials (database passwords, Kafka SASL credentials, Redis passwords, API keys) are injected via environment variables loaded from a `.env` file at runtime.

- `.env` is gitignored (already enforced in `.gitignore`)
- `.env.example` is committed with placeholder values documenting every required variable
- `docker-compose.yml` references variables via `${VAR_NAME}` syntax — no hardcoded values
- Services read all config from environment variables — no hardcoded defaults pointing to real hosts or using default/empty passwords

**`.env.example` required variables (Phase 1):**
```bash
# PostgreSQL
POSTGRES_USER=dispatch_user
POSTGRES_PASSWORD=changeme
POSTGRES_DB=dispatch_db

# Redis
REDIS_PASSWORD=changeme

# Kafka SASL (per-service credentials)
KAFKA_ADMIN_USERNAME=admin
KAFKA_ADMIN_PASSWORD=changeme
KAFKA_INGEST_USERNAME=ingest-service
KAFKA_INGEST_PASSWORD=changeme
KAFKA_DISPATCH_USERNAME=dispatch-service
KAFKA_DISPATCH_PASSWORD=changeme
KAFKA_NOTIFICATION_USERNAME=notification-service
KAFKA_NOTIFICATION_PASSWORD=changeme

# Service ports
INGEST_PORT=8001
DISPATCH_PORT=8080
NOTIFICATION_PORT=8002
```

**Phase 2:** Migrate to **HashiCorp Vault**. Services fetch secrets at startup via the Vault Agent sidecar (injects secrets as env vars or files) or the Vault SDK directly. Kubernetes auth method authenticates pods using their service account token. Dynamic secrets engine generates short-lived PostgreSQL and Kafka credentials on demand. Secret rotation does not require container restarts.

### Layer 2: Network Isolation (Phase 1)

Docker Compose named networks segment traffic so services only reach what they need.

**Network topology:**
```
kafka-net:      kafka, ingest, dispatch, notification
db-net:         postgres, redis, dispatch
frontend-net:   dispatch (port 8080 only), rider-ui
```

- The Notification Service has no access to `db-net` — it cannot reach PostgreSQL or Redis directly
- The Ingest Service has no access to `db-net` — it only writes to Kafka
- The Rider UI has no access to `kafka-net` or `db-net` — it only reaches the Dispatch HTTP endpoint
- No service exposes ports to the host except the three defined service ports (8001, 8080, 8002) and the Kafka admin port (9093 controller, localhost-only)

**Phase 2:** Kubernetes NetworkPolicy resources enforce the same isolation at the pod level. Egress is deny-by-default; only explicitly allowed routes are permitted.

### Layer 3: Kafka Authentication and Authorization

#### Phase 1 — Local docker-compose: SASL/PLAIN + PLAINTEXT

Local dev uses SASL/PLAIN over a plaintext connection. This is acceptable **only** because:
- All traffic is on an isolated Docker bridge network with no external exposure
- The risk model is fundamentally different from a networked environment
- Adding TLS to local dev adds certificate management complexity with zero security benefit

Each service gets its own credentials with topic-level ACLs. No service has wildcard topic access.

**Per-service Kafka ACLs (Phase 1 — local):**

| Service | Topic | Operations |
|---|---|---|
| `ingest-service` | `gps-pings` | `Write` |
| `dispatch-service` | `ride-events` | `Read`, `Write` |
| `dispatch-service` | `gps-pings` | `Read` |
| `notification-service` | `ride-events` | `Read` |
| `gateway-service` | `ride-events` | `Read` |
| `kafka-admin` (init only) | `*` | `Create`, `Describe` (topic creation only, not shared with any service) |

**Kafka superuser** (for topic creation at startup): a separate `admin` credential used only by the Kafka init container or topic-creation script. Not shared with any application service.

#### Phase 2 — EKS/Strimzi: SASL/SCRAM-SHA-512 + TLS (SASL_SSL)

Production and demo deployments on EKS use **SASL/SCRAM-SHA-512 over TLS**. This satisfies all three production best practices:

1. **Never SASL_PLAINTEXT in production**: The Strimzi listener uses `tls: true` — credentials and message payloads are encrypted in transit. A compromised pod cannot sniff credentials or data from other pods on the same node.

2. **SCRAM-SHA-512 over PLAIN**: SCRAM uses a challenge-response protocol — the broker never sees the raw password, and the stored credential is a salted hash. Even if the Kafka log directory or internal file system is compromised, stored SCRAM credentials cannot be reversed to recover the original password. PLAIN transmits credentials as base64 and provides no such protection.

3. **ACLs decouple authentication from authorization**: SASL proves identity; ACLs enforce what that identity is permitted to do. Each `KafkaUser` CRD defines both the authentication mechanism and the exact ACL rules — a service can only produce/consume the specific topics it owns.

Strimzi manages TLS certificates automatically via its built-in CA. Clients receive a truststore Secret injected by the operator — no manual certificate management.

**Strimzi Kafka listener configuration (Phase 2):**
```yaml
listeners:
  - name: tls
    port: 9093
    type: internal
    tls: true
    authentication:
      type: scram-sha-512
```

**Strimzi KafkaUser CRD with full ACL matrix (Phase 2):**
```yaml
# infra/k8s/kafka/users/ingest-service.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: ingest-service
  labels:
    strimzi.io/cluster: dispatch-cluster
spec:
  authentication:
    type: scram-sha-512          # SCRAM, not PLAIN
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: gps-pings
        operation: Write         # produce only — cannot read or write other topics
```

```yaml
# infra/k8s/kafka/users/dispatch-service.yaml
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: { type: topic, name: ride-events }
        operation: Read
      - resource: { type: topic, name: ride-events }
        operation: Write
      - resource: { type: topic, name: gps-pings }
        operation: Read          # CQRS read model consumer only
```

```yaml
# infra/k8s/kafka/users/notification-service.yaml
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: { type: topic, name: ride-events }
        operation: Read          # consume only — cannot produce
```

```yaml
# infra/k8s/kafka/users/gateway-service.yaml
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: { type: topic, name: ride-events }
        operation: Read          # consume only — cannot produce
```

Strimzi generates the SCRAM credentials automatically and stores them as Kubernetes Secrets. Services read the credentials from the Secret (injected by Vault Agent or mounted directly).

#### Phase 3 — mTLS (mutual TLS)

Phase 3 replaces SCRAM-SHA-512 with **mTLS** — both the broker and the client present certificates. The broker authenticates the client by certificate, not by password. This eliminates the credential management problem entirely and is the strongest available option.

Phase 3 requires a PKI infrastructure: cert-manager + Vault PKI secrets engine generating short-lived client certificates per service. Deferred until the platform has a dedicated SRE function.

### Layer 4: Transport Security (Phase 1 → Phase 2)

**Phase 1 (local docker-compose):** Internal traffic between containers is plaintext on isolated Docker networks. This is acceptable because all traffic is local and network-isolated. No external traffic reaches internal services directly.

**Phase 2 (staging/production):**
- TLS termination at the API Gateway for all external traffic
- mTLS between services and Kafka
- TLS on PostgreSQL and Redis connections
- All internal service-to-service HTTP uses HTTPS

### Layer 5: Application-Layer Security

#### Input Validation (Phase 1)

- All HTTP endpoints validate request bodies and return HTTP 422 on invalid input (already in requirements)
- Request body size is capped at 64 KB on all endpoints via middleware — prevents memory exhaustion from oversized payloads
- `driver_id` and `rider_id` fields are validated as non-empty strings with a maximum length of 128 characters
- GPS coordinates are validated to be within valid ranges (already in requirements)

#### Container Hardening (Phase 1)

- All service Dockerfiles run as a non-root user (`USER appuser` or equivalent)
- No service container runs with `--privileged` or mounts the Docker socket
- Base images are pinned to specific digest hashes, not floating tags (e.g., `python:3.11.9-slim` not `python:latest`)

#### Authentication & Authorization (Phase 2)

The Phase 1 skeleton has no authentication — it is a local dev tool, not exposed to real users. Phase 2 introduces JWT-based authentication:

- **Auth Service** (new bounded context): issues short-lived JWTs (15-minute expiry) on successful login
- **Gateway** validates JWT signature on all inbound requests; rejects with HTTP 401 if invalid or expired
- **Driver identity binding**: the JWT `sub` claim must match the `driver_id` in `POST /location` — a driver cannot post pings as another driver
- **Rider identity binding**: the JWT `sub` claim must match the `rider_id` in `POST /request-ride`
- **Service-to-service auth**: internal services use short-lived service account tokens, not the same JWTs issued to end users

#### Rate Limiting (Phase 2)

- `POST /location`: max 20 pings/second per `driver_id` (token bucket, enforced at Gateway)
- `POST /request-ride`: max 5 requests/minute per `rider_id` (prevents ride request flooding)
- Global rate limit: 10,000 requests/second across all endpoints (circuit breaker at Gateway)

#### Audit Logging (Phase 2)

- Every `TripAssigned` event includes the actor identity (`driver_id`, `rider_id`) — already in the Domain Event payload
- The Dispatch Service writes an audit log entry to PostgreSQL for every state transition on the `Trip` aggregate
- Audit log entries are append-only and include `actor_id`, `action`, `resource_id`, `occurred_at`, and `ip_address`

---

## Security Controls by Phase

| Control | Layer | Phase | Mechanism |
|---|---|---|---|
| No secrets in source control | Secrets | **Phase 1** | `.env` gitignored, `.env.example` committed |
| Per-service DB/Redis passwords | Secrets | **Phase 1** | `.env` variables, no shared credentials |
| Docker network segmentation | Network | **Phase 1** | Named networks with per-service membership |
| Kafka SASL/PLAIN per-service credentials | Kafka Auth | **Phase 1 (local only)** | Docker-isolated network; PLAIN acceptable locally; per-service ACLs enforced |
| Kafka SASL/SCRAM-SHA-512 + TLS (SASL_SSL) | Kafka Auth | **Phase 2 (EKS)** | Strimzi `KafkaUser` CRDs; SCRAM challenge-response; TLS-encrypted transport |
| Kafka per-service ACLs (operation-level) | Kafka AuthZ | **Phase 2 (EKS)** | `KafkaUser` CRDs with explicit `Read`/`Write` per topic per service |
| Non-root container users | Container | **Phase 1** | `USER` directive in all Dockerfiles |
| Pinned base image digests | Container | **Phase 1** | Specific version tags, no `latest` |
| Request body size cap (64 KB) | Input | **Phase 1** | FastAPI/Spring middleware |
| Field length validation | Input | **Phase 1** | Request schema validation |
| TLS termination at Gateway | Transport | Phase 2 | Gateway TLS, internal plaintext on private net |
| mTLS to Kafka | Transport | Phase 3 | Kafka TLS listener; cert-manager + Vault PKI; client certificates per service |
| JWT authentication | Auth | Phase 2 | Auth Service, Gateway validation |
| Driver/rider identity binding | AuthZ | Phase 2 | JWT `sub` claim == request identity field |
| Rate limiting | Input | Phase 2 | Token bucket at Gateway |
| Audit logging | Audit | Phase 2 | Append-only audit table in PostgreSQL |
| Secrets manager | Secrets | Phase 2 | HashiCorp Vault — Vault Agent sidecar, Kubernetes auth, dynamic secrets |
| Kubernetes NetworkPolicy | Network | Phase 3 | Deny-by-default egress |

---

## Known Gaps in Phase 1

| Gap | Risk | Accepted? | Mitigation |
|---|---|---|---|
| No JWT authentication | Any process can post as any driver/rider | Yes — local dev only, not exposed to real users | Document clearly; block external access via network isolation |
| Plaintext HTTP between containers | Traffic visible on Docker network | Yes — isolated Docker network, no external exposure | Phase 2 adds TLS |
| SASL/PLAIN (not SCRAM) to Kafka | Credentials vulnerable to dictionary attack if log directory compromised | Yes — **local dev only**; EKS uses SCRAM-SHA-512 | Phase 2 (EKS) uses SCRAM-SHA-512 via Strimzi `KafkaUser` CRDs |
| SASL_PLAINTEXT (no TLS) to Kafka | Credentials and payloads in plaintext on Docker network | Yes — **local dev only**; isolated Docker bridge network | Phase 2 (EKS) uses SASL_SSL (TLS-encrypted transport) |
| No rate limiting | Simulator or test can flood the Ingest Service | Yes — controlled dev environment | Smoke test validates at 10 pings/sec only |

---

## Threat Model (Summary)

| Threat | Mitigated in Phase 1? | Mitigation |
|---|---|---|
| Secret leakage via source control | ✅ Yes | `.gitignore`, `.env.example` pattern |
| Container escape / privilege escalation | ✅ Yes | Non-root users, no privileged containers |
| Lateral movement between services | ✅ Yes | Docker network segmentation |
| Unauthorized Kafka topic access | ✅ Phase 1 (local) | SASL/PLAIN ACLs per service |
| Unauthorized Kafka topic access | ✅ Phase 2 (EKS) | SCRAM-SHA-512 + operation-level ACLs via Strimzi `KafkaUser` CRDs |
| Kafka credential interception | ❌ Phase 1 (local — accepted) | Isolated Docker network; no external exposure |
| Kafka credential interception | ✅ Phase 2 (EKS) | SASL_SSL — TLS encrypts credentials and payloads in transit |
| Kafka credential brute force / log compromise | ❌ Phase 1 (local — accepted) | SASL/PLAIN; local dev only |
| Kafka credential brute force / log compromise | ✅ Phase 2 (EKS) | SCRAM-SHA-512 — salted hash; raw password never stored or transmitted |
| GPS coordinate spoofing (driver impersonation) | ❌ Phase 2 | JWT identity binding |
| Ride request flooding | ❌ Phase 2 | Rate limiting at Gateway |
| Man-in-the-middle on internal traffic | ❌ Phase 2 | mTLS |
| Credential brute force | ❌ Phase 2 | Rate limiting + account lockout at Auth Service |

---

## References

- [OWASP API Security Top 10](https://owasp.org/www-project-api-security/)
- [Apache Kafka Security — SASL/PLAIN](https://kafka.apache.org/documentation/#security_sasl_plain)
- [Apache Kafka Security — SASL/SCRAM](https://kafka.apache.org/documentation/#security_sasl_scram)
- [Strimzi KafkaUser CRD — SCRAM-SHA-512](https://strimzi.io/docs/operators/latest/configuring.html#type-KafkaUserScramSha512ClientAuthentication-reference)
- [Docker Network Security](https://docs.docker.com/network/)
- [JWT Best Practices — RFC 8725](https://www.rfc-editor.org/rfc/rfc8725)
- [HashiCorp Vault](https://developer.hashicorp.com/vault)
