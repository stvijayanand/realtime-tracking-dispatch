# Project Structure

> This project is greenfield. The structure below is the target layout for a microservices-based dispatch platform. Update as services are scaffolded.

## Top-Level Layout

```
realtime-tracking-dispatch/
├── services/                  # Individual microservices
│   ├── ingest/                # GPS ping ingestion service
│   ├── dispatch/              # Driver-to-order matching engine
│   ├── tracking/              # ETA computation and location state
│   ├── notification/          # Push notification delivery
│   └── gateway/               # API gateway / WebSocket hub for clients
├── shared/                    # Shared libraries/packages (DTOs, utils, errors)
├── infra/                     # Infrastructure-as-code
│   ├── docker/                # Dockerfiles per service
│   ├── k8s/                   # Kubernetes manifests
│   └── kafka/                 # Topic configs, schema registry
├── scripts/                   # Dev/ops helper scripts
├── docs/                      # Architecture diagrams, ADRs
├── .kiro/                     # Kiro specs and steering
│   ├── specs/                 # Feature specs (requirements, design, tasks)
│   └── steering/              # AI steering rules (this folder)
├── docker-compose.yml         # Local development environment
├── Requirements.md            # High-level product requirements
└── README.md
```

## Service Responsibilities

| Service        | Responsibility |
|----------------|----------------|
| `ingest`       | Receives GPS pings via HTTP/gRPC, publishes to Kafka `gps-pings` topic |
| `dispatch`     | Consumes order events, queries Redis for nearby drivers, assigns matches |
| `tracking`     | Consumes GPS pings, updates Redis geospatial index, computes and streams ETAs |
| `notification` | Consumes state-change events, sends FCM push notifications |
| `gateway`      | Authenticates clients, proxies REST calls, manages WebSocket/SSE connections |

## Conventions

- Each service is independently deployable with its own `Dockerfile` and config
- Shared types/DTOs live in `shared/` — never duplicate across services
- All inter-service communication goes through Kafka topics (async) or gRPC (sync queries only)
- Environment-specific config via environment variables — no hardcoded secrets or URLs
- Each service owns its own database schema; no cross-service direct DB access
- ADRs (Architecture Decision Records) go in `docs/adr/` with format `NNN-title.md`
