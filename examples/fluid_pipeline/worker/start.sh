#!/bin/sh
set -e

MYCELIUM_HOSTNAME=$(hostname -I | awk '{print $1}')
export MYCELIUM_HOSTNAME

# Start the Mycelium gossip node — peers to coordinator (the seed)
MYCELIUM_ROLE=node \
MYCELIUM_PORT=57000 \
MYCELIUM_HTTP_PORT="${MYCELIUM_HTTP_PORT:-8300}" \
MYCELIUM_PEERS="${MYCELIUM_PEERS:-coordinator:57000}" \
/usr/local/bin/mycelium-demo &

echo "[start] waiting for Mycelium node…"
until curl -sf "http://localhost:${MYCELIUM_HTTP_PORT:-8300}/health" > /dev/null 2>&1; do
    sleep 1
done
echo "[start] Mycelium node ready — $(hostname)"

exec python /app/worker.py
