# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

# System deps for reqwest (native-tls)
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation separately from the application code.
# Copy only the manifests and a stub lib first, build deps, then overwrite with
# the full source. This layer is reused on subsequent `docker build` calls when
# only application code (not Cargo.toml / Cargo.lock) changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src examples && \
    echo 'fn main(){}' > examples/three_node_demo.rs && \
    echo '' > src/lib.rs && \
    cargo build --release --features llm --example three_node_demo 2>/dev/null || true

# Build the real binary
COPY . .
# Touch the example so Cargo treats it as changed even though the timestamp
# of the stub was already in the layer cache.
RUN touch examples/three_node_demo.rs && \
    cargo build --release --features llm --example three_node_demo

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder \
    /build/target/release/examples/three_node_demo \
    /usr/local/bin/mycelium-demo

# Gossip port (TCP, peer-to-peer)
EXPOSE 57000
# Mycelium HTTP gateway (REST + SSE, language bridges + observability)
EXPOSE 8300
# Interactive chat UI (llm role only)
EXPOSE 8080
# Management dashboard (mgmt role only)
EXPOSE 8090

ENTRYPOINT ["mycelium-demo"]
