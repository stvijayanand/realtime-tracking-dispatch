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
- Services communicate asynchronously via Kafka events; synchronous REST/gRPC only for query paths
- Push notifications must be fire-and-forget with retry logic — never block the dispatch critical path
