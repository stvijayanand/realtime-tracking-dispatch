# ADR 007: Demo Infrastructure Cost Strategy — Spin-Up / Tear-Down on AWS

**Status:** Accepted  
**Date:** 2026-05-17  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Bounded Contexts), ADR 004 (Security Model)

---

## Context

This platform is built to FAANG-scale production standards and deployed on AWS EKS. However, it is also a demonstration system — it does not serve real users and should not incur AWS costs when not actively being shown. The goal is to make no concessions on technology choices while paying as close to zero as possible when the demo is idle.

Two constraints must be satisfied simultaneously:
1. **Technology fidelity**: The AWS deployment must use production-grade technology — EKS, Kubernetes-native Kafka, PostgreSQL, Redis — not toy substitutes or managed black boxes with no demo value.
2. **Cost discipline**: AWS billing stops completely when the demo is not running. No idle resources, no forgotten instances.

---

## Decision

### Principle: Local-First, Cloud-Optional

The system runs in two modes:

| Mode | Environment | Cost |
|---|---|---|
| **Development / CI** | `docker-compose up` on a laptop | $0 |
| **Demo** | AWS EKS via `make demo-up` | ~$0.30–0.80 per 4-hour session |

All development, testing, and iteration happens locally. AWS is only provisioned for live demos and torn down immediately after.

### Infrastructure-as-Code: Terraform Spin-Up / Tear-Down

All AWS resources are defined in `infra/terraform/`. A single command creates the full environment; a single command destroys it and stops all billing.

```bash
make demo-up    # terraform apply  — full AWS environment ready in ~10 min
make demo-down  # terraform destroy — all resources deleted, billing stops
```

The Terraform state is the source of truth. If `terraform show` lists resources, they are costing money.

### AWS Service Choices and Right-Sizing

#### EKS (Kubernetes)
- Control plane: `$0.10/hour` — unavoidable while cluster exists
- Worker nodes: **Spot instances** (`t3.large`) — 60–90% cheaper than On-Demand
- For a demo cluster running all services plus Strimzi Kafka, 2 `t3.large` Spot nodes are sufficient
- Node group uses `capacity_type = "SPOT"` in Terraform

#### Kafka: Strimzi Operator on EKS (not MSK)

Kafka runs on EKS via the **Strimzi Kafka Operator** — no separate AWS Kafka billing line.

**Why Strimzi over MSK Serverless:**

| Dimension | Strimzi on EKS | MSK Serverless |
|---|---|---|
| Cost | $0 Kafka billing — runs on existing Spot nodes | Per partition-hour + per GB transferred |
| Demo value | Shows Kubernetes-native Kafka operations (CRDs, operator) | Black box — nothing to demonstrate |
| Consistency | Same Apache Kafka binary as local docker-compose | Different runtime, different edge behaviour |
| Cloud portability | Works on EKS, GKE, AKS, bare metal | AWS-only |
| KRaft support | Full KRaft via `process.roles: broker,controller` | Internal KRaft, not configurable |

Strimzi is installed via Helm (`strimzi/strimzi-kafka-operator`). The Kafka cluster, topics, and per-service users are all Kubernetes CRDs:

```yaml
# infra/k8s/kafka/kafka-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: dispatch-cluster
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    config:
      process.roles: broker,controller
      min.insync.replicas: 2
    storage:
      type: persistent-claim
      size: 50Gi
      class: gp3
    listeners:
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512    # SCRAM-SHA-512 over TLS (SASL_SSL) — never PLAIN over PLAINTEXT in production
```

```yaml
# infra/k8s/kafka/topics/gps-pings.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: gps-pings
  labels:
    strimzi.io/cluster: dispatch-cluster
spec:
  partitions: 12
  replicas: 3
  config:
    min.insync.replicas: "2"
```

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
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: gps-pings
        operation: Write
```

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
    type: scram-sha-512          # SCRAM-SHA-512 — salted hash; raw password never stored or transmitted
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: gps-pings
        operation: Write         # produce only — cannot read or write any other topic
```

The `KAFKA_BOOTSTRAP_SERVERS` env var points to `dispatch-cluster-kafka-bootstrap:9093` (TLS port) — the Strimzi-managed Kubernetes Service. Services load the CA certificate from the Strimzi-generated Secret (injected by Vault Agent or mounted as a volume).

#### RDS PostgreSQL
- Use **Aurora Serverless v2** (`db.serverless`, PostgreSQL-compatible)
- Scales to 0 ACUs when idle — you pay only for storage (~$0.10/GB-month) when the demo is not actively running queries
- Minimum ACU capacity: 0.5 (scales up on first query, scales back to 0 after idle timeout)
- **Why not RDS `db.t3.micro`**: A stopped RDS instance still charges for storage and cannot be destroyed/recreated quickly. Aurora Serverless v2 scales to near-zero automatically without manual stop/start.

#### Redis: Self-Hosted StatefulSet on EKS (not ElastiCache)

Redis runs as a Kubernetes `StatefulSet` with an EBS gp3 `PersistentVolumeClaim` — the same `redis:7.2-alpine` image as local docker-compose.

**Why not ElastiCache:**

| Dimension | Self-hosted Redis StatefulSet | ElastiCache |
|---|---|---|
| Cost | $0 Redis billing — runs on existing Spot nodes; ~$0.08/GB-month EBS only | ~$0.017/hour even at zero load |
| Demo value | Shows stateful workload management on Kubernetes (StatefulSet, PVC, headless Service) | Black box — nothing to demonstrate |
| Consistency | Same `redis:7.2-alpine` image as local docker-compose | Different runtime |
| Cloud portability | Works on any Kubernetes cluster | AWS-only |

#### Observability: Self-Hosted on EKS (not AWS X-Ray / Amazon Managed Prometheus)

Jaeger, Prometheus, and Grafana run as Kubernetes pods on EKS — the same images as local docker-compose. AWS X-Ray and Amazon Managed Prometheus are black boxes with no demo value: you send data to them and they display it, but nothing about them is visible or demonstrable.

Self-hosted observability on EKS demonstrates:
- You understand OpenTelemetry SDK instrumentation (not just "we used X-Ray")
- You can write PromQL queries and build Grafana dashboards
- You understand distributed tracing across async Kafka boundaries

Cost: $0 extra — runs on existing Spot nodes.

#### ElastiCache → Removed

ElastiCache is removed from the stack. Redis runs self-hosted (see above).

#### Self-Hosted on EKS (no additional AWS cost)
The following run as Kubernetes pods on the EKS cluster — no separate managed service:
- **Strimzi Kafka** (3-broker KRaft cluster)
- **Redis** (`redis:7.2-alpine` StatefulSet with EBS gp3 PVC)
- **HashiCorp Vault** (`vault:1.15`)
- **Confluent Schema Registry** (self-hosted)
- **Jaeger** (traces — replaces AWS X-Ray)
- **Prometheus + Grafana** (metrics — replaces Amazon Managed Prometheus)
- **PgBouncer** (connection pooling)

All of these are destroyed with the cluster at no extra cost. The only AWS-managed services remaining are EKS (control plane) and Aurora Serverless v2 (PostgreSQL).

### Dead-Man's Switch: Auto-Destroy Lambda

A scheduled AWS Lambda function runs every 6 hours and checks whether the EKS cluster has been active for more than 6 hours without a `demo-extend` heartbeat. If so, it triggers `terraform destroy` automatically.

This prevents the most common cost failure mode: forgetting to run `make demo-down` after a demo.

The Lambda is defined in `infra/terraform/modules/auto-destroy/` and is deployed as part of `make demo-up`. It is destroyed by `make demo-down`.

### Estimated Costs

| Scenario | Duration | Estimated cost |
|---|---|---|
| Local docker-compose | Unlimited | $0 |
| AWS demo session | 4 hours | ~$0.30–0.80 |
| AWS demo session | 8 hours | ~$0.80–1.60 |
| Auto-destroy fires (forgotten demo) | 6 hours max | ~$0.50–1.20 |
| AWS resources left running 1 week | 168 hours | ~$15–25 (prevented by auto-destroy) |

The only billable AWS services are: EKS control plane ($0.10/hour) + EKS Spot nodes (~$0.05–0.10/hour for 2× `t3.large` Spot) + Aurora Serverless v2 (near-zero when idle) + ECR storage (negligible). Everything else runs on the Spot nodes at no extra cost.

### Terraform Module Structure

```
infra/terraform/
  main.tf                    — root module, calls all child modules
  variables.tf               — input variables (region, cluster name, tags)
  outputs.tf                 — kubeconfig, Kafka bootstrap service, RDS endpoint
  modules/
    eks/                     — EKS cluster + Spot node group
    strimzi/                 — Helm release for Strimzi operator
    rds/                     — Aurora Serverless v2 PostgreSQL cluster
    ecr/                     — ECR repositories for all service images
    auto-destroy/            — Lambda + EventBridge rule for dead-man's switch
  environments/
    demo/                    — demo-specific tfvars (minimal sizing, Spot instances)

infra/k8s/
  kafka/
    kafka-cluster.yaml       — Kafka CRD (3-broker KRaft, SCRAM-SHA-512)
    topics/                  — KafkaTopic CRDs for all topics
    users/                   — KafkaUser CRDs with per-service ACLs
  redis/
    statefulset.yaml         — Redis StatefulSet with EBS gp3 PVC
    service.yaml             — Headless Service for Redis
  observability/
    jaeger.yaml              — Jaeger all-in-one Deployment + Service
    prometheus.yaml          — Prometheus Deployment + ConfigMap (scrape configs)
    grafana.yaml             — Grafana Deployment + ConfigMap (data sources)
```

### Makefile Targets

```makefile
# Local development
up:           docker-compose up -d
down:         docker-compose down
logs:         docker-compose logs -f
build:        go build ./... (all Go services) + mvn package (Dispatch)
test:         go test ./... + mvn test
lint:         golangci-lint run + mvn checkstyle:check

# Demo infrastructure
demo-up:      cd infra/terraform && terraform init && terraform apply -auto-approve -var-file=environments/demo/demo.tfvars
demo-down:    cd infra/terraform && terraform destroy -auto-approve -var-file=environments/demo/demo.tfvars
demo-status:  cd infra/terraform && terraform show | grep -E "resource|id ="
demo-extend:  aws lambda invoke --function-name dispatch-demo-heartbeat /dev/null
demo-cost:    cd infra/terraform && terraform plan -var-file=environments/demo/demo.tfvars | grep -c "will be created"

# OpenAPI
check-openapi: scripts/generate_openapi.sh && git diff --exit-code services/*/openapi.json
```

---

## Consequences

### Positive
- Zero AWS cost when demo is not running
- Strimzi removes the MSK billing line entirely — Kafka runs on existing Spot nodes at no extra cost
- Self-hosted Redis removes the ElastiCache billing line — Redis runs on existing Spot nodes at no extra cost
- Self-hosted Jaeger + Prometheus + Grafana removes AWS X-Ray and Amazon Managed Prometheus — observability runs on existing Spot nodes at no extra cost
- Only two AWS-managed services remain: EKS control plane and Aurora Serverless v2
- Demonstrates Kubernetes-native operations across the full stack: Strimzi CRDs, Redis StatefulSet, Jaeger/Prometheus/Grafana deployments — higher demo value than managed black boxes
- Identical images and configuration between local docker-compose and EKS — no environment drift
- Cloud-portable: the same Kubernetes manifests work on GKE, AKS, or bare-metal
- Single command to create and destroy the full environment
- Auto-destroy Lambda prevents forgotten idle resources
- Aurora Serverless v2 scales to near-zero automatically, providing a second layer of cost protection

### Negative / Trade-offs
- `make demo-up` takes ~12–15 minutes (EKS cluster creation + Strimzi operator + Kafka cluster bootstrap)
- Strimzi + Redis StatefulSet + observability stack requires 2 `t3.large` Spot nodes instead of 1
- You own broker health and Redis persistence — the Strimzi operator and Kubernetes handle restarts, but you need to understand failure modes; managed services would handle this transparently
- Aurora Serverless v2 has a cold-start latency (~1–2 seconds) on the first query after scaling to zero — acceptable for a demo
- Terraform state must be stored remotely (S3 + DynamoDB lock table) to support the auto-destroy Lambda; this adds a small S3 cost (~$0.023/GB-month, negligible)

### Neutral
- The docker-compose environment and the EKS environment use identical container images and identical Kafka configuration — the same `make build` produces images that run in both
- `KAFKA_BOOTSTRAP_SERVERS` changes from an MSK endpoint to `dispatch-cluster-kafka-bootstrap:9092` (Strimzi Kubernetes Service) — a one-line env var change
- `REDIS_HOST` changes from an ElastiCache endpoint to `redis:6379` (Kubernetes Service) — identical to local docker-compose
- Kubernetes manifests in `infra/k8s/` are environment-agnostic; only the Terraform variables change between local and demo

---

## References

- [Strimzi Kafka Operator](https://strimzi.io/)
- [Strimzi KRaft support](https://strimzi.io/blog/2023/09/11/kafka-kraft-migration/)
- [Strimzi Helm chart](https://artifacthub.io/packages/helm/strimzi/strimzi-kafka-operator)
- [Aurora Serverless v2 pricing](https://aws.amazon.com/rds/aurora/pricing/)
- [EKS Spot instances](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html)
- [Terraform AWS provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [HashiCorp Vault on Kubernetes](https://developer.hashicorp.com/vault/docs/platform/k8s)
- [Jaeger on Kubernetes](https://www.jaegertracing.io/docs/latest/operator/)
- [KEDA Kafka scaler](https://keda.sh/docs/latest/scalers/apache-kafka/)
