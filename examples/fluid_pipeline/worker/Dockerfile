# Stage 1: compile the Mycelium gossip node binary
FROM rust:1.85-slim AS rust-builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src examples && \
    echo 'fn main(){}' > examples/three_node_demo.rs && \
    echo '' > src/lib.rs && \
    cargo build --release --example three_node_demo 2>/dev/null || true
COPY . .
RUN touch examples/three_node_demo.rs && \
    cargo build --release --example three_node_demo

# Stage 2: Python + Mycelium binary runtime
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=rust-builder \
    /build/target/release/examples/three_node_demo \
    /usr/local/bin/mycelium-demo
WORKDIR /app
COPY examples/fluid_pipeline/worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mycelium-py/ /mycelium-py/
RUN pip install --no-cache-dir /mycelium-py
COPY examples/fluid_pipeline/worker/ .
RUN chmod +x start.sh
CMD ["./start.sh"]
