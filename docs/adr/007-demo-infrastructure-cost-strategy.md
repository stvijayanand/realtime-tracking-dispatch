# ADR 007: Demo Infrastructure Cost Strategy — Spin-Up / Tear-Down on AWS

**Status:** Accepted  
**Date:** 2026-05-17  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Bounded Contexts), ADR 004 (Security Model)

---

## Context

This platform is built to FAANG-scale production standards and deployed on AWS EKS. However, it is also a demonstration system — it does not serve real users and should not incur AWS costs when not actively being shown. The goal is to make no concessions on technology choices while paying as close to zero as possible when the demo is idle.

Two constraints must be satisfied simultaneously:
1. **Technology fidelity**: The AWS deployment must use production-grade technology — EKS, Kubernetes-native Kafka, RDS, ElastiCache — not toy substitutes.
2. **Cost discipline**: AWS billing stops completely when the demo is not running. No idle resources, no forgotten instances.

---

## Decision

### Principle: Local-First, Cloud-Optional

The system runs in two modes:

| Mode | Environment | Cost |
|---|---|---|
| **Development / CI** | `docker-compose up` on a laptop | $0 |
| **Demo** | AWS EKS via `make demo-up` | ~$0.50–1.50 per 4-hour session |

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
      - name: plain
        port: 9092
        type: internal
        tls: false
        authentication:
          type: scram-sha-512
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

The `KAFKA_BOOTSTRAP_SERVERS` env var points to `dispatch-cluster-kafka-bootstrap:9092` — the Strimzi-managed Kubernetes Service. This is the only change from the local docker-compose config.

#### RDS PostgreSQL
- Use **Aurora Serverless v2** (`db.serverless`, PostgreSQL-compatible)
- Scales to 0 ACUs when idle — you pay only for storage (~$0.10/GB-month) when the demo is not actively running queries
- Minimum ACU capacity: 0.5 (scales up on first query, scales back to 0 after idle timeout)
- **Why not RDS `db.t3.micro`**: A stopped RDS instance still charges for storage and cannot be destroyed/recreated quickly. Aurora Serverless v2 scales to near-zero automatically without manual stop/start.

#### ElastiCache (Redis)
- `cache.t3.micro` — ~$0.017/hour
- Tear down with everything else via `terraform destroy`
- Phase 1 Redis is reserved but not written to; Phase 2 activates GEOADD

#### Self-Hosted on EKS (no additional AWS cost)
The following run as Kubernetes pods on the EKS cluster — no separate managed service:
- **Strimzi Kafka** (3-broker KRaft cluster)
- **HashiCorp Vault** (`vault:1.15`)
- **Confluent Schema Registry** (self-hosted)
- **Jaeger** (traces)
- **Prometheus + Grafana** (metrics)
- **PgBouncer** (connection pooling)

All of these are destroyed with the cluster at no extra cost.

### Dead-Man's Switch: Auto-Destroy Lambda

A scheduled AWS Lambda function runs every 6 hours and checks whether the EKS cluster has been active for more than 6 hours without a `demo-extend` heartbeat. If so, it triggers `terraform destroy` automatically.

This prevents the most common cost failure mode: forgetting to run `make demo-down` after a demo.

The Lambda is defined in `infra/terraform/modules/auto-destroy/` and is deployed as part of `make demo-up`. It is destroyed by `make demo-down`.

### Estimated Costs

| Scenario | Duration | Estimated cost |
|---|---|---|
| Local docker-compose | Unlimited | $0 |
| AWS demo session | 4 hours | ~$0.50–1.50 |
| AWS demo session | 8 hours | ~$1.50–3.00 |
| Auto-destroy fires (forgotten demo) | 6 hours max | ~$1.00–2.00 |
| AWS resources left running 1 week | 168 hours | ~$30–50 (prevented by auto-destroy) |

Cost reduction vs. MSK Serverless: removing the MSK billing line saves ~$0.50–1.00 per session at demo traffic levels.

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
    elasticache/             — Redis cache.t3.micro
    ecr/                     — ECR repositories for all service images
    auto-destroy/            — Lambda + EventBridge rule for dead-man's switch
  environments/
    demo/                    — demo-specific tfvars (minimal sizing, Spot instances)

infra/k8s/
  kafka/
    kafka-cluster.yaml       — Kafka CRD (3-broker KRaft, SCRAM-SHA-512)
    topics/                  — KafkaTopic CRDs for all topics
    users/                   — KafkaUser CRDs with per-service ACLs
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
- Demonstrates Kubernetes-native Kafka operations (CRDs, operator pattern) — higher demo value than a managed black box
- Identical Apache Kafka binary in local docker-compose and on EKS — no environment drift
- Cloud-portable: the same Strimzi manifests work on GKE, AKS, or bare-metal Kubernetes
- Single command to create and destroy the full environment
- Auto-destroy Lambda prevents forgotten idle resources
- Aurora Serverless v2 scales to near-zero automatically, providing a second layer of cost protection

### Negative / Trade-offs
- `make demo-up` takes ~12–15 minutes (EKS cluster creation + Strimzi operator + Kafka cluster bootstrap)
- Strimzi requires more EKS node capacity than MSK (Kafka brokers run as pods); 2 `t3.large` Spot nodes instead of 1
- You own broker health — if a broker pod crashes, the Strimzi operator restarts it, but you need to understand why; MSK would handle this transparently
- Aurora Serverless v2 has a cold-start latency (~1–2 seconds) on the first query after scaling to zero — acceptable for a demo
- Terraform state must be stored remotely (S3 + DynamoDB lock table) to support the auto-destroy Lambda; this adds a small S3 cost (~$0.023/GB-month, negligible)

### Neutral
- The docker-compose environment and the EKS environment use identical container images and identical Kafka configuration — the same `make build` produces images that run in both
- `KAFKA_BOOTSTRAP_SERVERS` changes from an MSK endpoint to `dispatch-cluster-kafka-bootstrap:9092` (Strimzi Kubernetes Service) — a one-line env var change
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
