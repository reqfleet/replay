FROM gcr.io/distroless/static-debian12

WORKDIR /app

# Copy the pre-built binary from the host
ARG BIN_DIR=bin
ARG BINARY=replay
COPY ${BIN_DIR}/${BINARY} /app/replay

# Copy default configuration
COPY config.yaml .

# Expose metrics port
EXPOSE 9102

ENTRYPOINT ["/app/replay"]
