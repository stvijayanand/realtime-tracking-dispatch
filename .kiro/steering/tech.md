# Tech Stack

> This project targets FAANG-scale, production-grade distributed systems. Every technology choice is made with that bar in mind. Local dev uses docker-compose; production targets AWS EKS. Update this file as decisions are finalized.

---

## Language & Runtime

| Service | Language | Rationale |
|---|---|---|
| `ingest` | **Go** | True goroutine concurrency, sub-ms HTTP latency, handles 10k+ concurrent GPS pings per instance. Single static binary in a distroless image. |
| `dispatch` | **Java 21 (Spring Boot 3.x)** | Virtual threads (Project Loom) give Go-level concurrency with JPA/JDBC ecosystem. Owns the stateful Trip aggregate and PostgreSQL writes. |
| `tracking` | **Go** | High-frequency geospatial computation, Redis GEOADD writes at GPS ping rate. |
| `notification` | **Go** | Fire-and-forget Kafka consumer + FCM/APNs HTTP calls. No state, no DB. |
| `gateway` | **Go** | Manages 100k+ long-lived WebSocket connections per instance. gorilla/websocket or nhooyr.io/websocket. |

**Why not Python**: The GIL limits true parallelism. Python FastAPI is fine for demos but cannot sustain 10k+ concurrent GPS pings per instance without horizontal scaling that Go handles in a single process.

---

## Message Streaming

### Apache Kafka (KRaft mode) — Primary event bus

- **Local dev**: 3-broker KRaft cluster in docker-compose (`confluentinc/cp-kafka:7.6.1`), no ZooKeeper
- **Production**: AWS MSK (Managed Streaming for Kafka) — fully managed, multi-AZ, KRaft-native from MSK 2.x
- **Replication**: `replication.factor=3`, `min.insync.replicas=2` on all topics — demonstrates fault tolerance
- **Producer config**: `acks=all`, `enable.idempotence=true` on every producer
- **Schema enforcement**: Confluent Schema Registry + **Avro** for all Domain Events — schema evolution with backward/forward compatibility enforced at publish time; broker rejects malformed messages before they reach consumers
- Topics: `gps-pings`, `ride-events`, `dispatch-commands`, `notifications`

### AWS Kinesis — Analytics / GPS history pipeline (Phase 2)

- GPS pings are dual-published: Kafka for real-time processing, Kinesis Data Streams for the analytics pipeline
- Kinesis Firehose delivers to S3 → Athena for historical GPS track queries
- Demonstrates the Kafka vs. Kinesis trade-off: Kafka for low-latency event-driven microservices; Kinesis for high-volume stream-to-storage pipelines
- Kinesis shard key = `driver_id` (same ordering guarantee as Kafka partition key)

**Kafka vs. Kinesis decision boundary**: All inter-service Domain Events use Kafka. Kinesis is used exclusively for the analytics/data-lake pipeline where managed throughput scaling (shard splitting) and native AWS integration (Firehose → S3) are more valuable than Kafka's consumer group flexibility.

---

## Data Storage

### PostgreSQL — Trip aggregate, audit log (Dispatch Service)

- **Local dev**: `postgres:16-alpine` (pinned)
- **Production**: AWS RDS PostgreSQL Multi-AZ with read replicas
- **Connection pooling**: **PgBouncer** in transaction pooling mode between Dispatch Service and PostgreSQL — multiplexes thousands of application connections onto a small pool of actual DB connections. This is the production failure mode PostgreSQL hits before CPU does.
- **Query optimization**: All queries on the `trips` table use covering indexes. `idx_trips_status` and `idx_trips_updated_at` are defined from Phase 1. `EXPLAIN ANALYZE` output is committed alongside any new query in a `docs/query-plans/` directory.
- **Schema migrations**: Flyway (Java) — versioned, repeatable, baseline migrations. Never run raw DDL in application code.

### Redis — Geospatial driver index, ETA cache, WebSocket session registry

- **Local dev**: `redis:7.2-alpine` (pinned), password-protected
- **Production**: AWS ElastiCache for Redis (cluster mode enabled, Multi-AZ)
- **Geospatial**: `GEOADD dispatch:drivers <lng> <lat> <driver_id>` — Dispatch Service CQRS read model (Phase 2). `GEORADIUS` for nearest-driver queries.
- **ETA cache**: `SET eta:{trip_id} <seconds> EX 30` — 30-second TTL, refreshed on each `ETAUpdated` event
- **WebSocket session registry**: `HSET gateway:sessions:{rider_id} instance_id connection_id` — enables multi-instance Gateway fan-out via Redis Pub/Sub. Phase 1 uses in-memory map; Phase 2 uses Redis.

### DynamoDB — Notification delivery log, idempotency table (Phase 2)

- **Use case**: The Notification Service's `processed_events` deduplication table (ADR 003) is a natural DynamoDB fit — high write throughput, single-key lookups, TTL-based expiry, no relational joins needed.
- **Schema**: `PK=event_id`, `TTL=processed_at + 24h`. `ConditionExpression: attribute_not_exists(event_id)` on write — atomic idempotency check without a distributed lock.
- **Why not PostgreSQL**: The deduplication table is write-heavy (one write per consumed Kafka message), append-only, and never queried relationally. DynamoDB's single-digit millisecond writes at any scale are the right fit. PostgreSQL would become a bottleneck here at high message rates.
- **Local dev**: DynamoDB Local (`amazon/dynamodb-local`) in docker-compose — identical API, no AWS account needed for local testing.

---

## Real-Time Client Streaming

### Gateway Service — WebSocket/SSE hub (Kafka consumer + protocol bridge)

The Gateway is a **Kafka consumer** like any other bounded context. Kafka fans out events to it via a dedicated consumer group (`gateway-consumer-group`). The Gateway's sole job is **protocol translation**: Kafka Domain Event → WebSocket frame pushed to the connected rider client.

```
Kafka ride-events topic
    ├── dispatch-consumer-group  → Dispatch Service
    ├── notification-consumer-group → Notification Service
    └── gateway-consumer-group  → Gateway Service
                                      → session registry lookup: rider_id → WebSocket conn
                                      → push event payload over WebSocket
                                      → Rider UI receives live update
```

The Gateway does **not** fan out events — Kafka does. The Gateway translates protocol and routes to the correct connection. It owns:
- WebSocket connection lifecycle (accept, heartbeat, graceful disconnect)
- Session registry: `rider_id → WebSocket connection` (in-memory Phase 1, Redis Pub/Sub Phase 2 for multi-instance)
- Kafka consumer for `ride-events` (`TripAssigned`, `TripCancelled`, `TripCompleted`) and `ETAUpdated`
- No domain logic, no DB writes — pure infrastructure

**Why a dedicated Gateway service**: Separates stateful long-lived connections (WebSocket) from stateless request/response services. Allows independent scaling — you scale Gateway instances for connection count, Dispatch instances for trip throughput.

---

## Observability (OpenTelemetry — from Phase 1)

Production systems are not observable by accident. OTel is instrumented from the first service, not retrofitted.

- **Traces**: `trace_id` and `span_id` propagated through Kafka message headers — every hop (HTTP → Kafka produce → Kafka consume → DB write) is a span in the same trace. A single GPS ping is traceable end-to-end.
- **Metrics**: Prometheus-format `/metrics` endpoint on every service — Kafka consumer lag, HTTP p50/p95/p99 latency, DB query duration, WebSocket connection count
- **Logs**: Structured JSON with `trace_id` field — logs correlate to traces in Jaeger/Grafana
- **Local dev**: Jaeger (traces) + Prometheus + Grafana in docker-compose
- **Production**: AWS X-Ray (traces) + Amazon Managed Prometheus + Grafana

---

## Infrastructure

### Local Development
- **docker-compose** — single `docker-compose up` starts the full stack: 3-broker Kafka cluster, Schema Registry, PostgreSQL + PgBouncer, Redis, DynamoDB Local, Jaeger, Prometheus, Grafana, all services

### Production / Demo on AWS
- **AWS EKS** (Elastic Kubernetes Service) — managed Kubernetes, integrates with IAM, ALB Ingress Controller, EBS/EFS volumes
- **MSK Serverless** — Kafka with no broker sizing; scales to near-zero at demo traffic levels; same API as provisioned MSK; use provisioned `kafka.t3.small` 3-broker cluster only for sustained production load
- **Aurora Serverless v2** — PostgreSQL-compatible; scales to 0 ACUs when idle; cold-start ~1–2s on first query; use provisioned RDS only for sustained production load
- **ElastiCache `cache.t3.micro`** — torn down with everything else via `terraform destroy`
- **Spot instances** (`t3.large`) for EKS worker nodes — 60–90% cheaper than On-Demand; sufficient for a demo cluster running 5–6 small pods
- **Horizontal Pod Autoscaler (HPA)** on all services — scale on CPU and custom metrics (Kafka consumer lag via KEDA)
- **KEDA** (Kubernetes Event-Driven Autoscaling) — scales Kafka consumer pods based on consumer group lag
- **AWS ALB Ingress Controller** — TLS termination, path-based routing, WAF integration

### Cost Strategy: Spin-Up / Tear-Down
- **Zero cost when idle** — `make demo-down` runs `terraform destroy` and stops all AWS billing
- **~$1–2 per 4-hour demo session** — EKS Spot + MSK Serverless + Aurora Serverless v2 + ElastiCache
- **Dead-man's switch** — a scheduled Lambda auto-destroys the environment after 6 hours without a heartbeat, preventing forgotten idle resources
- **Self-hosted on EKS** (no extra AWS cost): HashiCorp Vault, Schema Registry, Jaeger, Prometheus, Grafana, PgBouncer
- See `docs/adr/007-demo-infrastructure-cost-strategy.md` for full cost breakdown and Terraform module structure

### Container Standards
- All Dockerfiles use **distroless** or `scratch` base images for Go services — no shell, no package manager, minimal attack surface
- Java services use `eclipse-temurin:21-jre-jammy` (pinned digest)
- All containers run as non-root users
- Multi-stage builds: build stage compiles, final stage copies only the binary

---

## Secrets Management

| Environment | Mechanism |
|---|---|
| Local dev | `.env` file (gitignored), loaded from `.env.example` |
| Staging / Production | **HashiCorp Vault** — services fetch secrets at startup via the Vault Agent sidecar or the Vault SDK; AppRole or Kubernetes auth method; dynamic secret rotation without container restarts |

**Why HashiCorp Vault**: Cloud-agnostic — works identically on AWS EKS, GCP GKE, Azure AKS, and bare-metal Kubernetes. Vault's dynamic secrets engine can generate short-lived PostgreSQL credentials and Kafka SASL credentials on demand, rotating them automatically. The Vault Agent sidecar injects secrets as environment variables or files into the pod without any SDK changes in application code.

---

## Key Architectural Constraints

- All GPS ping ingestion must go through Kafka — never write directly to DB from the ingest endpoint
- Driver location state lives in Redis; PostgreSQL is the source of truth for Trip/order records
- Services communicate asynchronously via Kafka Domain Events; synchronous REST/gRPC only for non-hot-path operations (health checks, admin queries)
- **Kafka fans out events to consumer groups** — the Gateway Service is a Kafka consumer like any other; it does not fan out events, it translates Kafka events to WebSocket frames
- Push notifications are fire-and-forget with retry logic — never block the dispatch critical path
- Each service is a Bounded Context — domain objects never shared via common library; only infrastructure types in `shared/`
- Kafka messages use Avro schema (Schema Registry) — `event_type` field in envelope distinguishes Domain Events on the same topic
- The `Trip` aggregate is owned exclusively by the Dispatch service
- The Dispatch service maintains a local CQRS read model of driver locations (Redis GEOADD) by consuming `LocationPingReceived` events — never calls Tracking synchronously (see ADR 005)
- The Trip state machine is persisted in PostgreSQL by the Dispatch service — source of truth for saga state
- Distributed transactions use the Saga pattern: choreography Phase 1/2, orchestration Phase 3+ (see ADR 006)
- Compensating domain events (`TripCancelled`, `TripExpired`, `PaymentFailed`) are first-class domain events — modelled in code from Phase 1
- DynamoDB is used for high-write-throughput, single-key-lookup tables (notification dedup, idempotency) — not for relational data
- All query plans for PostgreSQL queries must be documented with `EXPLAIN ANALYZE` output in `docs/query-plans/`

## Security Constraints

- No secrets in source control — ever. `.env` gitignored, `.env.example` committed
- Production secrets via HashiCorp Vault (Vault Agent sidecar or Vault SDK; AppRole / Kubernetes auth; dynamic secret rotation)
- Each service has its own Kafka SASL credentials with ACLs restricted to only the topics it owns
- Docker Compose named networks segment traffic: `kafka-net`, `db-net`, `frontend-net`
- All Dockerfiles run as non-root users with pinned base image versions
- All HTTP endpoints enforce 64 KB maximum request body size
- Services fail fast with a descriptive error if a required environment variable is missing
- Phase 2: mTLS between services and Kafka (MSK TLS listener); JWT auth at Gateway; rate limiting via Kong/ALB WAF
- See `docs/adr/004-security-model.md` for the full security model and phase controls

## Common Commands

```bash
# ── Local development ──────────────────────────────────────────────────────
# Start full local stack (Kafka 3-broker, Schema Registry, PgBouncer, Redis,
# DynamoDB Local, Jaeger, Prometheus, Grafana, all services)
make up                  # docker-compose up -d

# Stop local stack (data volumes preserved)
make down                # docker-compose down

# Tail all service logs
make logs                # docker-compose logs -f

# Build all services
make build               # go build ./... + mvn package

# Run all tests
make test                # go test ./... + mvn test

# Lint
make lint                # golangci-lint run + mvn checkstyle:check

# Kafka topic list (local)
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# View distributed traces
open http://localhost:16686  # Jaeger UI

# View metrics dashboards
open http://localhost:3000   # Grafana (admin/admin)

# ── Demo infrastructure (AWS) ──────────────────────────────────────────────
# Spin up full AWS demo environment (~10 min; ~$1–2 per 4-hour session)
make demo-up             # terraform apply — EKS + MSK Serverless + Aurora Serverless v2 + ElastiCache

# Tear down all AWS resources (billing stops immediately)
make demo-down           # terraform destroy — run this when demo is finished

# Check what AWS resources are currently provisioned (if any = costs money)
make demo-status         # terraform show

# Reset the auto-destroy timer (prevents Lambda from tearing down a running demo)
make demo-extend         # sends heartbeat to auto-destroy Lambda

# Estimate cost of current Terraform plan
make demo-cost           # terraform plan | count resources to be created

# ── OpenAPI ────────────────────────────────────────────────────────────────
# Regenerate and validate all committed OpenAPI specs
make check-openapi       # scripts/generate_openapi.sh + git diff --exit-code
```
