#!/bin/sh
set -e

# Derive this container's gossip address from the kernel-reported IP
MYCELIUM_HOSTNAME=$(hostname -I | awk '{print $1}')
export MYCELIUM_HOSTNAME

# Start the Mycelium gossip node as a background sidecar
MYCELIUM_ROLE=node \
MYCELIUM_PORT=57000 \
MYCELIUM_HTTP_PORT="${MYCELIUM_HTTP_PORT:-8300}" \
MYCELIUM_PEERS="${MYCELIUM_PEERS:-}" \
/usr/local/bin/mycelium-demo &

# Wait until the HTTP gateway is accepting requests
echo "[start] waiting for Mycelium node…"
until curl -sf "http://localhost:${MYCELIUM_HTTP_PORT:-8300}/health" > /dev/null 2>&1; do
    sleep 1
done
echo "[start] Mycelium node ready"

exec python /app/coordinator.py
