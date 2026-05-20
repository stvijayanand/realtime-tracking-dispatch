# =============================================================================
# Ingest Service — Multi-stage Dockerfile
# =============================================================================
# NOTE: The spec mandates CGO_ENABLED=0 with gcr.io/distroless/static-debian12,
# but confluent-kafka-go v2 requires CGO for its embedded librdkafka C bindings.
# A pure-static build with CGO_ENABLED=0 is incompatible with confluent-kafka-go.
#
# Correct approach used here:
#   Stage 1 (builder): golang:1.22-alpine with gcc + musl-dev for CGO support.
#                      CGO_ENABLED=1 with -extldflags="-static" produces a
#                      fully statically-linked binary via musl libc.
#   Stage 2 (runtime): gcr.io/distroless/cc-debian12:nonroot — includes libc
#                      for CGO binaries; runs as nonroot (uid 65532) by default.
#
# Requirements: 2.11, 10.6, 10.7
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
# CGO_ENABLED=1  — required for confluent-kafka-go's librdkafka C bindings.
# -extldflags="-static" — links against musl libc statically so the binary
#                         is self-contained and runs in a minimal final image.
# -ldflags="-s -w" — strips debug symbols and DWARF info to reduce binary size.
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w -extldflags=-static" \
    -o /app/ingest \
    ./

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# gcr.io/distroless/cc-debian12:nonroot includes libc for CGO binaries and
# runs as nonroot (uid 65532) by default — no shell, no package manager,
# minimal attack surface.
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

# Copy only the compiled binary from the build stage.
COPY --from=builder /app/ingest /app/ingest

# Expose the service port (actual port is set via SERVICE_PORT env var).
EXPOSE 8001

ENTRYPOINT ["/app/ingest"]
