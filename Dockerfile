# Build stage
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace + member manifests
COPY Cargo.toml Cargo.lock ./
COPY shard-router/Cargo.toml ./shard-router/Cargo.toml
COPY search-kv-proxy/Cargo.toml ./search-kv-proxy/Cargo.toml

# Create dummy sources for every workspace member to cache dependencies.
# `cargo build` here builds only the tieredkv package (+ shard-router dep); the
# search-kv-proxy member just needs to exist for the workspace to load.
RUN mkdir -p src shard-router/src search-kv-proxy/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "" > shard-router/src/lib.rs && \
    echo "fn main() {}" > search-kv-proxy/src/main.rs && \
    cargo build --release --features azure,cluster && \
    rm -rf src shard-router/src search-kv-proxy/src

# Copy real source code (all workspace members)
COPY src ./src
COPY shard-router ./shard-router
COPY search-kv-proxy ./search-kv-proxy

# Build the actual application (tieredkv only)
RUN cargo build --release --features azure,cluster

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/tieredkv /usr/local/bin/

# Create data directory
RUN mkdir -p /data

# Set environment variables
ENV RUST_LOG=info
ENV DATA_DIR=/data
ENV HTTP_PORT=8080

# Expose HTTP port
EXPOSE 8080

# Run the application
CMD ["tieredkv"]
