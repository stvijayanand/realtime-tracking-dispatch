# =============================================================================
# Dispatch Service — Multi-stage Dockerfile
# =============================================================================
# Stage 1: Build
#   - eclipse-temurin:21-jdk-jammy provides the full JDK for Maven compilation.
#   - Maven wrapper is used so no Maven installation is required on the host.
#
# Stage 2: Runtime
#   - eclipse-temurin:21-jre-jammy — minimal JRE image (pinned version tag).
#   - Runs as non-root user 'appuser' (uid/gid created in this stage).
#   - Only the compiled JAR is copied from the build stage.
#
# Requirements: 3.12, 10.6, 10.7
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-jammy AS builder

# Install Maven directly — avoids the Maven wrapper downloading Maven at build
# time, which fails behind corporate SSL proxies.
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy POM first to leverage Docker layer caching for dependency downloads.
COPY pom.xml pom.xml
COPY .mvn .mvn

# Download dependencies (cached layer — only re-runs when pom.xml changes).
RUN mvn dependency:go-offline -B --no-transfer-progress

# Copy source code and build the JAR.
COPY src src
RUN mvn package -DskipTests -B --no-transfer-progress

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# eclipse-temurin:21-jre-jammy — pinned version, not 'latest'.
# Minimal JRE image: no JDK tools, no compiler, reduced attack surface.
FROM eclipse-temurin:21-jre-jammy

# Create a non-root user and group for running the application.
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy only the compiled JAR from the build stage.
COPY --from=builder /build/target/dispatch-service-*.jar app.jar

# Set ownership so appuser can read the JAR.
RUN chown appuser:appuser app.jar

# Switch to non-root user (Requirement 10.6).
USER appuser

# Expose the service port (actual port is set via SERVICE_PORT env var).
EXPOSE 8080

# Use exec form to ensure the JVM receives SIGTERM for graceful shutdown.
ENTRYPOINT ["java", "-jar", "app.jar"]
