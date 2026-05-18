# Prompts

## End to End Skeleton

Starting with the first phase of implementation - End to End Skeleton

Goal: one driver ping reaches one rider notification, end-to-end, in docker-compose. No Kubernetes, no Flink yet. Prove the core data flow before adding complexity.

Tasks

Monorepo scaffold — two top-level dirs:
services/
(FastAPI + Spring Boot) and
infra/
(docker-compose, later Helm)

FastAPI Location Ingestion Gateway: POST /location endpoint → writes to Kafka (local Redpanda container)

Spring Boot Dispatch Service stub: Kafka consumer → hardcoded nearest-driver logic → publishes trip.assigned event

FastAPI Notification Service stub: consumes trip.assigned → logs to stdout (real APNs/FCM later)

Driver simulator script (Python): emits 10 GPS pings/sec per simulated driver along a GeoJSON route

Minimal React rider page: static map (Leaflet), "Request Ride" button hits Dispatch stub

docker-compose up spins everything including Redpanda, Redis, Postgres

Deliverables
Driver pings → rider notification, all in docker-compose
OpenAPI specs for both services generated and committed

## Architecture diagram

Can you create an architecture diagram showing data flow and add it to Readme with relevant details?

## Flow clarification

RUI->>DISP: POST /request-ride {rider_id, pickup_location}

    DISP->>RP: Publish ride-request → ride-events topic

    DISP-->>RUI: HTTP 202 {trip_id}

    RP->>DISP: Consume ride-request from ride-events

    Note over DISP: Hardcoded nearest-driver logic<br/>(static in-memory driver list)

    DISP->>RP: Publish trip.assigned → ride-events topic



This is from the README, why does DISP have to publish ride-request to RP and then consume it again from RP?


## Reverting back to round-trip

No, revert back to 'Keep the Kafka round-trip' approach for scale

## DDD

I think we are desinging this project with Microservices, but are using DDD?

### DDD decision

Yes, create the adr. Are you making necessary changes everywhere to reflect this decision?

## Consistency

How are you handling consistency with this architecture?

## Idempotency

Are you including idempotency?

## Security

I'd like like security baked in from the start. How are you handling security?

## Cross Context reads

Querying data across multiple contexts synchronously can create cascading failures and latency spikes

Are you going to use CQRS and external foreign data replication?

## Transaction boundaries

Are you using the Saga Pattern (Orchestration or Choreography) to manage distributed transactions and execute compensating actions upon failures?

### Clarification on Saga steps

You said 'The saga is short (3 steps) and linear — choreography handles it cleanly.'

Is this short for the whole system or for just phase 1? 

#### More clarification on notification

In ADR 006, you said '| Notification delivery fails | `NotificationFailed` | Log to DLQ; do not block trip progression |'

How does the rider know if notification fails that a driver has been assigned?

## Design Patterns

I'd like you to implement best practices including Low level design patterns.

## Kraft or ZooKeper

For Kafka, will we need KRaft (Kafka Raft) for coordination?

### Redpanda -> Kafka + KRaft

I'd like to use Kafka with Kraft, instead. Please update. everywhere.

## My primary goal

I have asked you to move from Redpanda to Kafka + KRaft because I want to build FAANG scale, production-grade, real-world distributed systems that I can demonstrate. I want you to keep this in mind and suggest better technologies/design wherever you see fit. Make the suggestions ongoing as we build iteratively.

### Adding more technologies

Yes I’d like you to add this clarification to the changes you have recommended to improve the current stack to FAANG-scale production. One change I want to make is AWS secret manager instead of Hashicorp Vault. Also I would like to appropriately fit the following backend competencies into our design: ├── Event Streaming: Apache Kafka, AWS Kinesis ├── Containerization: Docker, Kubernetes, AWS EKS └── Data Storage: PostgreSQL (Query Optimization), Redis, DynamoDB

### Vault

I have changed my mind, I'd like you to please use Hashicorp Vault over AWS secrets manager.

### Observability

I see a section for Observability, but I still see stdout in requirements. Please explain.

## AWS cost

Please keep in mind that I want to keep costs down, especially AWS costs, while making no concessions on the right technology choice for a FAANG scale distributed system. I would like to spin up this demo only when needed to save costs. Any guidance please?
 
### Strimzi

I'd like to use Strimzi Kafka on EKS  over MSK. Please give your recommendation