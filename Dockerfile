# Dockerfile for FinBase (FDAAE)
FROM scratch

# Copy root CA certificates and static binary into container
COPY ca-certificates.crt fdaae /

EXPOSE 8080

ENTRYPOINT ["/fdaae"]
