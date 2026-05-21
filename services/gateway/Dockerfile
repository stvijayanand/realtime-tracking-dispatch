# =============================================================================
# Gateway Service — Multi-stage Dockerfile
# =============================================================================
# NOTE: confluent-kafka-go v2 requires CGO for its embedded librdkafka bindings.
# CGO_ENABLED=0 is incompatible with confluent-kafka-go. Correct approach:
#   Stage 1: golang:1.22-alpine with gcc + musl-dev (CGO_ENABLED=1)
#            -extldflags="-static" produces a fully statically-linked binary
#   Stage 2: gcr.io/distroless/cc-debian12:nonroot — includes libc for CGO
#            binaries; runs as nonroot (uid 65532) by default
#
# Requirements: 9.6, 10.6, 10.7
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# Install C compiler and musl-dev for CGO (required by confluent-kafka-go/librdkafka).
RUN apk add --no-cache gcc musl-dev

WORKDIR /build

# Copy go.mod and go.sum first to leverage Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy source code.
COPY . .

# Build a statically-linked binary.
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w -extldflags=-static" \
    -o /app/gateway \
    ./

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# gcr.io/distroless/cc-debian12:nonroot — includes libc for CGO binaries,
# runs as nonroot (uid 65532) by default, no shell, no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

# Copy only the compiled binary from the build stage.
COPY --from=builder /app/gateway /app/gateway

# Expose the service port (actual port is set via SERVICE_PORT env var).
EXPOSE 8003

ENTRYPOINT ["/app/gateway"]
