# ADR 007: Demo Infrastructure Cost Strategy — Spin-Up / Tear-Down on AWS

**Status:** Accepted  
**Date:** 2026-05-17  
**Deciders:** Platform Engineering Team  
**Relates to:** ADR 001 (Bounded Contexts), ADR 004 (Security Model)

---

## Context

This platform is built to FAANG-scale production standards and deployed on AWS EKS. However, it is also a demonstration system — it does not serve real users and should not incur AWS costs when not actively being shown. The goal is to make no concessions on technology choices while paying as close to zero as possible when the demo is idle.

Two constraints must be satisfied simultaneously:
1. **Technology fidelity**: The AWS deployment must use the same services a production system would — EKS, MSK, RDS, ElastiCache — not toy substitutes.
2. **Cost discipline**: AWS billing stops completely when the demo is not running. No idle resources, no forgotten instances.

---

## Decision

### Principle: Local-First, Cloud-Optional

The system runs in two modes:

| Mode | Environment | Cost |
|---|---|---|
| **Development / CI** | `docker-compose up` on a laptop | $0 |
| **Demo** | AWS EKS via `make demo-up` | ~$1–2 per 4-hour session |

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
- For a demo cluster running 5–6 small Go/Java pods, one `t3.large` Spot node is sufficient
- Node group uses `capacity_type = "SPOT"` in Terraform

#### MSK (Managed Kafka)
- Use **MSK Serverless** — charges per partition-hour and per GB transferred
- For a demo with low traffic, cost is near zero
- Provides the real MSK API, real KRaft mode, real SASL/PLAIN — identical to a production MSK cluster
- No broker sizing decisions; MSK Serverless scales automatically
- **Why not a provisioned 3-broker cluster**: A `kafka.t3.small` 3-broker cluster costs ~$0.27/hour even at zero load. MSK Serverless costs effectively nothing at demo traffic levels.

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
- HashiCorp Vault (self-hosted, `vault:1.15`)
- Confluent Schema Registry (self-hosted)
- Jaeger (traces)
- Prometheus + Grafana (metrics)
- PgBouncer (connection pooling)

This is intentional: these components are production-grade but do not require managed AWS services. Running them on EKS means they are destroyed with the cluster at no extra cost.

### Dead-Man's Switch: Auto-Destroy Lambda

A scheduled AWS Lambda function runs every 6 hours and checks whether the EKS cluster has been active for more than 6 hours without a `demo-extend` heartbeat. If so, it triggers `terraform destroy` automatically.

This prevents the most common cost failure mode: forgetting to run `make demo-down` after a demo.

The Lambda is defined in `infra/terraform/modules/auto-destroy/` and is deployed as part of `make demo-up`. It is destroyed by `make demo-down`.

### Estimated Costs

| Scenario | Duration | Estimated cost |
|---|---|---|
| Local docker-compose | Unlimited | $0 |
| AWS demo session | 4 hours | ~$1–2 |
| AWS demo session | 8 hours | ~$3–5 |
| Auto-destroy fires (forgotten demo) | 6 hours max | ~$2–3 |
| AWS resources left running 1 week | 168 hours | ~$50–80 (prevented by auto-destroy) |

### Terraform Module Structure

```
infra/terraform/
  main.tf                    — root module, calls all child modules
  variables.tf               — input variables (region, cluster name, tags)
  outputs.tf                 — kubeconfig, MSK bootstrap servers, RDS endpoint
  modules/
    eks/                     — EKS cluster + Spot node group
    msk/                     — MSK Serverless cluster + SASL/PLAIN credentials
    rds/                     — Aurora Serverless v2 PostgreSQL cluster
    elasticache/             — Redis cache.t3.micro
    ecr/                     — ECR repositories for all service images
    auto-destroy/            — Lambda + EventBridge rule for dead-man's switch
  environments/
    demo/                    — demo-specific tfvars (minimal sizing, Spot instances)
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
demo-extend:  aws lambda invoke --function-name demo-heartbeat /dev/null  # resets auto-destroy timer
demo-cost:    cd infra/terraform && terraform plan -var-file=environments/demo/demo.tfvars | grep -c "will be created"

# OpenAPI
check-openapi: scripts/generate_openapi.sh && git diff --exit-code services/*/openapi.json
```

---

## Consequences

### Positive
- Zero AWS cost when demo is not running
- No concessions on technology — EKS, MSK, RDS, ElastiCache are the same services used in production
- Single command to create and destroy the full environment
- Auto-destroy Lambda prevents forgotten idle resources
- Aurora Serverless v2 and MSK Serverless scale to near-zero automatically, providing a second layer of cost protection even if `make demo-down` is not run immediately

### Negative / Trade-offs
- `make demo-up` takes ~10 minutes (EKS cluster creation is the bottleneck)
- MSK Serverless has slightly higher per-message latency than a provisioned cluster at high throughput — acceptable for a demo, not for production
- Aurora Serverless v2 has a cold-start latency (~1–2 seconds) on the first query after scaling to zero — acceptable for a demo
- Terraform state must be stored remotely (S3 + DynamoDB lock table) to support the auto-destroy Lambda; this adds a small S3 cost (~$0.023/GB-month, negligible)

### Neutral
- The docker-compose environment and the AWS environment use identical container images — the same `make build` produces images that run in both
- Kubernetes manifests in `infra/k8s/` are environment-agnostic; only the Terraform variables change between local and demo

---

## References

- [MSK Serverless pricing](https://aws.amazon.com/msk/pricing/)
- [Aurora Serverless v2 pricing](https://aws.amazon.com/rds/aurora/pricing/)
- [EKS Spot instances](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html)
- [Terraform AWS provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [HashiCorp Vault on Kubernetes](https://developer.hashicorp.com/vault/docs/platform/k8s)
