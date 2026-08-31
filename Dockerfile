# Multi-stage Dockerfile for FinBase (FDAAE)
# Stage 1: Build binary with CGO_ENABLED=0
FROM golang:1.24-alpine AS builder

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

# Copy root CA certificates for HTTPS external API requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary from builder
COPY --from=builder /app/fdaae /fdaae

EXPOSE 8080

ENTRYPOINT ["/fdaae"]
