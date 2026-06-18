#!/usr/bin/env bash
# Regenerate the committed echo_component.wasm fixture.
# Requires: rustup target add wasm32-wasip2
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip2
cp target/wasm32-wasip2/release/echo_component.wasm ../echo_component.wasm
echo "wrote ../echo_component.wasm ($(wc -c < ../echo_component.wasm) bytes)"
