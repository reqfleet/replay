FROM gcr.io/distroless/static-debian12

WORKDIR /app

# Copy the pre-built binary from the host
COPY bin/replay .

# Copy default configuration
COPY config.yaml .

# Expose metrics port
EXPOSE 9102

ENTRYPOINT ["/app/replay"]
