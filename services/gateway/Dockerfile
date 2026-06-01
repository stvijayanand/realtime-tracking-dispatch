# =============================================================================
# Gateway Service — Multi-stage Dockerfile
# =============================================================================
# confluent-kafka-go v2 vendors librdkafka compiled against glibc. Building on
# Alpine (musl libc) causes linker errors. Use golang:1.25-bookworm (Debian,
# glibc) as the builder. Runtime: gcr.io/distroless/cc-debian12:nonroot.
#
# Requirements: 9.6, 10.6, 10.7
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.25-bookworm AS builder

RUN apt-get update -qq && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /app/gateway \
    ./

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update -qq && apt-get install -y --no-install-recommends wget ca-certificates && rm -rf /var/lib/apt/lists/* && groupadd -r nonroot && useradd -r -g nonroot nonroot

WORKDIR /app

COPY --from=builder /app/gateway /app/gateway

USER nonroot

EXPOSE 8003

ENTRYPOINT ["/app/gateway"]
