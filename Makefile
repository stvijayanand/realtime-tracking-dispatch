# =============================================================================
# Real-Time Ride/Delivery Tracking & Dispatch Platform
# =============================================================================
# Local development targets use docker-compose.
# Demo infrastructure targets use Terraform to spin up / tear down AWS resources.
#
# COST REMINDER: AWS resources cost money while running.
#   make demo-up   → starts billing (~$1–2 per 4-hour session)
#   make demo-down → stops ALL billing (run this when done)
#
# See docs/adr/007-demo-infrastructure-cost-strategy.md for full cost breakdown.
# =============================================================================

TERRAFORM_DIR     := infra/terraform
DEMO_TFVARS       := environments/demo/demo.tfvars
SERVICES_GO       := services/ingest services/notification services/gateway services/tracking
SERVICE_JAVA      := services/dispatch

.PHONY: up down logs build test lint \
        demo-up demo-down demo-status demo-extend demo-cost \
        check-openapi help

# ── Local development ─────────────────────────────────────────────────────────

## up: Start the full local stack (Kafka 3-broker, Schema Registry, PgBouncer,
##     Redis, DynamoDB Local, Jaeger, Prometheus, Grafana, all services)
up:
	docker-compose up -d
	@echo ""
	@echo "Local stack is up."
	@echo "  Jaeger:     http://localhost:16686"
	@echo "  Grafana:    http://localhost:3000  (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Ingest:     http://localhost:8001/health"
	@echo "  Dispatch:   http://localhost:8080/health"
	@echo "  Notify:     http://localhost:8002/health"
	@echo "  Gateway:    http://localhost:8003/health"

## down: Stop the local stack (data volumes are preserved)
down:
	docker-compose down

## logs: Tail all service logs
logs:
	docker-compose logs -f

## build: Build all services (Go static binaries + Java JAR)
build:
	@echo "Building Go services..."
	@for svc in $(SERVICES_GO); do \
		echo "  $$svc"; \
		cd $$svc && CGO_ENABLED=0 go build ./... && cd -; \
	done
	@echo "Building Dispatch Service (Java)..."
	cd $(SERVICE_JAVA) && mvn -q package -DskipTests && cd -
	@echo "Build complete."

## test: Run all tests (Go + Java)
test:
	@echo "Testing Go services..."
	@for svc in $(SERVICES_GO); do \
		echo "  $$svc"; \
		cd $$svc && go test ./... && cd -; \
	done
	@echo "Testing Dispatch Service (Java)..."
	cd $(SERVICE_JAVA) && mvn -q test && cd -
	@echo "All tests passed."

## lint: Run linters (golangci-lint + Maven Checkstyle)
lint:
	@echo "Linting Go services..."
	@for svc in $(SERVICES_GO); do \
		echo "  $$svc"; \
		cd $$svc && golangci-lint run ./... && cd -; \
	done
	@echo "Linting Dispatch Service (Java)..."
	cd $(SERVICE_JAVA) && mvn -q checkstyle:check && cd -
	@echo "Lint passed."

# ── Demo infrastructure (AWS) ─────────────────────────────────────────────────

## demo-up: Provision full AWS demo environment.
##          Takes ~10 minutes. Costs ~$1–2 per 4-hour session.
##          ALWAYS run 'make demo-down' when finished.
demo-up:
	@echo "============================================================"
	@echo "  Provisioning AWS demo environment..."
	@echo "  This will cost ~\$$1–2 per 4-hour session."
	@echo "  Run 'make demo-down' when finished to stop billing."
	@echo "============================================================"
	cd $(TERRAFORM_DIR) && terraform init -input=false
	cd $(TERRAFORM_DIR) && terraform apply -auto-approve -var-file=$(DEMO_TFVARS)
	@echo ""
	@echo "Demo environment ready."
	@echo "Run 'make demo-down' when finished. Auto-destroy fires after 6 hours."

## demo-down: Destroy ALL AWS resources. Billing stops immediately.
demo-down:
	@echo "============================================================"
	@echo "  Destroying all AWS demo resources..."
	@echo "  Billing will stop once this completes."
	@echo "============================================================"
	cd $(TERRAFORM_DIR) && terraform destroy -auto-approve -var-file=$(DEMO_TFVARS)
	@echo ""
	@echo "All AWS resources destroyed. Billing stopped."

## demo-status: Show currently provisioned AWS resources.
##              If this lists resources, they are costing money.
demo-status:
	@echo "Currently provisioned AWS resources (if any = costs money):"
	cd $(TERRAFORM_DIR) && terraform show | grep -E "^resource|id\s*=" || echo "  No resources provisioned."

## demo-extend: Reset the auto-destroy timer (prevents Lambda from tearing
##              down a demo that is still in use).
demo-extend:
	@echo "Sending heartbeat to auto-destroy Lambda..."
	aws lambda invoke \
		--function-name dispatch-demo-heartbeat \
		--payload '{}' \
		/dev/null
	@echo "Auto-destroy timer reset. Environment will persist for another 6 hours."

## demo-cost: Estimate the number of AWS resources that would be created.
demo-cost:
	cd $(TERRAFORM_DIR) && terraform plan -var-file=$(DEMO_TFVARS) 2>&1 | \
		grep -E "will be created|will be destroyed|Plan:"

# ── OpenAPI ───────────────────────────────────────────────────────────────────

## check-openapi: Regenerate all OpenAPI specs and fail if any differ from
##                the committed version (CI enforcement).
check-openapi:
	@echo "Regenerating OpenAPI specs..."
	scripts/generate_openapi.sh
	@echo "Checking for uncommitted changes..."
	git diff --exit-code services/ingest/openapi.json \
	                     services/dispatch/openapi.json \
	                     services/notification/openapi.json
	@echo "All OpenAPI specs are up to date."

# ── Help ──────────────────────────────────────────────────────────────────────

## help: Show this help message
help:
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Local development:"
	@grep -E '^## (up|down|logs|build|test|lint):' $(MAKEFILE_LIST) | \
		sed 's/^## /  /' | column -t -s ':'
	@echo ""
	@echo "Demo infrastructure (AWS — costs money while running):"
	@grep -E '^## demo-' $(MAKEFILE_LIST) | \
		sed 's/^## /  /' | column -t -s ':'
	@echo ""
	@echo "OpenAPI:"
	@grep -E '^## check-openapi:' $(MAKEFILE_LIST) | \
		sed 's/^## /  /' | column -t -s ':'
	@echo ""
