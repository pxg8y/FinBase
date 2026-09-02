# Dockerfile for FinBase (FDAAE)
FROM scratch

WORKDIR /app

# Copy root CA certificates and static binary into container
COPY ca-certificates.crt /
COPY fdaae /app/fdaae

EXPOSE 8080

ENTRYPOINT ["/app/fdaae"]
