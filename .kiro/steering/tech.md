# Tech Stack

> This project is in early/greenfield stage. The stack below reflects sensible defaults for a high-throughput real-time dispatch platform. Update this file as decisions are finalized.

## Recommended Stack

### Language & Runtime
- **Primary**: Go or Java (high concurrency, low latency) — or Node.js if team preference
- Confirm language choice before scaffolding services

### Message Streaming
- **Apache Kafka** — ingestion of GPS pings at scale, event-driven state changes
- Topics: `gps-pings`, `ride-events`, `dispatch-commands`, `notifications`

### Databases
- **Redis** — geospatial indexing for driver locations (`GEOADD`/`GEORADIUS`), ETA caching, session state
- **PostgreSQL** — persistent storage for orders, rides, users, audit logs
- **TimescaleDB** (optional) — time-series GPS history if analytics are needed

### Real-Time Streaming to Clients
- **WebSockets** or **Server-Sent Events (SSE)** — push ETA updates to customer apps
- Consider a dedicated gateway service (e.g., using Socket.IO or native WS)

### Push Notifications
- **Firebase Cloud Messaging (FCM)** — mobile push notifications
- Wrap in an internal notification service to abstract provider

### Infrastructure
- **Docker** — containerization for all services
- **Kubernetes** — orchestration at scale
- **API Gateway** — single entry point for external clients (e.g., Kong, AWS API Gateway)

## Common Commands

> Populate these as the project is scaffolded.

```bash
# Build
# e.g., go build ./... or mvn package

# Run locally
# e.g., docker-compose up

# Run tests
# e.g., go test ./... or mvn test

# Lint
# e.g., golangci-lint run
```

## Key Architectural Constraints

- All GPS ping ingestion must go through Kafka — never write directly to DB from the ingest endpoint
- Driver location state lives in Redis; PostgreSQL is the source of truth for order/ride records
- Services communicate asynchronously via Kafka Domain Events; synchronous REST/gRPC only for cross-context query paths
- Push notifications must be fire-and-forget with retry logic — never block the dispatch critical path
- Each service is a Bounded Context — domain objects (Trip, DriverLocation, Notification) are never shared via a common library; only infrastructure types live in `shared/`
- Kafka messages must include an `event_type` field to distinguish Domain Events on the same topic (e.g., `TripRequested`, `TripAssigned` on `ride-events`)
- The `Trip` aggregate is owned exclusively by the Dispatch service — no other service reads or writes the Trip table directly
- Cross-context queries (e.g., Dispatch querying DriverLocation) go through an internal HTTP/gRPC API, never direct DB or Redis access from another service — **exception**: Dispatch uses a CQRS local read model instead of synchronous queries (see ADR 005)
- See `docs/adr/001-microservices-with-ddd-bounded-contexts.md` for the full decision record

## Security Constraints

- No secrets, passwords, or API keys in source control — ever. Use `.env` (gitignored) loaded from `.env.example`
- All service configuration (broker addresses, credentials, ports) via environment variables — no hardcoded defaults
- Each service has its own Kafka SASL credentials with ACLs restricted to only the topics it owns (principle of least privilege)
- Docker Compose named networks segment traffic: services only join networks they require (`kafka-net`, `db-net`, `frontend-net`)
- All Dockerfiles run as non-root users and use pinned base image versions — never `latest`
- All HTTP endpoints enforce a 64 KB maximum request body size
- Services must fail fast with a descriptive error if a required environment variable is missing — never start with insecure defaults
- See `docs/adr/004-security-model.md` for the full security model, threat model, and Phase 2 controls (JWT auth, TLS, rate limiting)
