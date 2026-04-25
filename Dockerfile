# Build stage - build from repo root: docker build -f warp-storage-engine/Dockerfile .
FROM golang:1.24.3-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY warp-storage-engine/go.mod warp-storage-engine/go.sum ./warp-storage-engine/
WORKDIR /app/warp-storage-engine
RUN go mod download

COPY warp-storage-engine/ .

RUN CGO_ENABLED=0 GOOS=linux go build -tags vector -a -installsuffix cgo -ldflags '-extldflags "-static"' -o warp-storage-engine ./cmd/warp-storage-engine

# Final stage
FROM alpine:latest

# Install wget for health checks
RUN apk --no-cache add wget ca-certificates

COPY --from=builder /app/warp-storage-engine/warp-storage-engine /warp-storage-engine

# Copy web UI files and API documentation
COPY warp-storage-engine/web /web
COPY warp-storage-engine/api /api

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/warp-storage-engine"]
