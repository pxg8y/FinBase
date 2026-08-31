# Multi-stage Dockerfile for FinBase (FDAAE)
# Stage 1: Build binary with CGO_ENABLED=0
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy dependency files and download modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source files
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o fdaae main.go

# Stage 2: Minimal scratch image
FROM scratch

# Set working directory to /app so relative DB_PATH (e.g., data/finbase.db) resolves to /app/data/finbase.db
WORKDIR /app

# Copy root CA certificates for HTTPS external API requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary from builder
COPY --from=builder /app/fdaae /app/fdaae

EXPOSE 8080

ENTRYPOINT ["/app/fdaae"]
