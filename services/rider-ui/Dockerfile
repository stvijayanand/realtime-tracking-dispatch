# =============================================================================
# Rider UI — Multi-stage Dockerfile
# =============================================================================
# Stage 1: Build
#   - node:20.12-alpine builds the React/Vite static bundle
#   - VITE_DISPATCH_URL is injected as a build-time ARG
#
# Stage 2: Serve
#   - nginx:1.25-alpine serves the static files
#   - Runs as non-root user (nginx user, uid 101)
#   - Pinned versions, never 'latest'
#
# Requirements: 10.6, 10.7
# =============================================================================

FROM node:20.12-alpine AS builder

WORKDIR /app

# Copy package files first for layer caching.
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

# Build-time env var for Dispatch Service URL (Requirement 6.6).
ARG VITE_DISPATCH_URL=http://localhost:8080
ENV VITE_DISPATCH_URL=$VITE_DISPATCH_URL

COPY . .
RUN npm run build

# ── Stage 2: Serve ────────────────────────────────────────────────────────────
FROM nginx:1.25-alpine

# Copy built static files.
COPY --from=builder /app/dist /usr/share/nginx/html

# Custom nginx config for SPA routing (all paths → index.html).
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
