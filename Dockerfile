FROM gcr.io/distroless/static-debian12

WORKDIR /app

# Copy the pre-built binary from the host
ARG BIN_DIR=bin
ARG BINARY=replay
COPY ${BIN_DIR}/${BINARY} /app/replay

ENTRYPOINT ["/app/replay"]
