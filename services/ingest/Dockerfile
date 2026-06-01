# =============================================================================
# Ingest Service — Multi-stage Dockerfile
# =============================================================================
# confluent-kafka-go v2 vendors librdkafka compiled against glibc (the .a file
# is named librdkafka_glibc_linux_amd64.a). Building on Alpine (musl libc)
# causes undefined references to glibc-specific symbols (__strdup, fopen64,
# getcontext, etc.) when linking statically.
#
# Solution: use golang:1.25-bookworm (Debian, glibc) as the builder so the
# linker finds the glibc symbols librdkafka expects. The binary is dynamically
# linked against glibc and runs in gcr.io/distroless/cc-debian12:nonroot which
# ships the same glibc runtime.
#
# Requirements: 2.11, 10.6, 10.7
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.25-bookworm AS builder

# Install gcc for CGO (required by confluent-kafka-go/librdkafka).
RUN apt-get update -qq && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy go.mod and go.sum first to leverage Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy source code.
COPY . .

# Build a dynamically-linked binary against glibc.
# CGO_ENABLED=1  — required for confluent-kafka-go's librdkafka C bindings.
# -ldflags="-s -w" — strips debug symbols to reduce binary size.
# No -extldflags=-static: librdkafka_glibc requires dynamic glibc linking.
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /app/ingest \
    ./

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# Use debian-slim instead of distroless so wget is available for health checks.
# Still runs as nonroot, minimal attack surface.
FROM debian:bookworm-slim

RUN apt-get update -qq && apt-get install -y --no-install-recommends wget ca-certificates && rm -rf /var/lib/apt/lists/* && groupadd -r nonroot && useradd -r -g nonroot nonroot

WORKDIR /app

COPY --from=builder /app/ingest /app/ingest

USER nonroot

EXPOSE 8001

ENTRYPOINT ["/app/ingest"]
