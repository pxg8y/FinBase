# Multi-stage Dockerfile for FinBase (FDAAE)
# Stage 1: Build binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o fdaae main.go

# Stage 2: Distroless minimal image with built-in CA certificate bundle
FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /app/fdaae /app/fdaae

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/app/fdaae", "-healthcheck"]

ENTRYPOINT ["/app/fdaae"]
