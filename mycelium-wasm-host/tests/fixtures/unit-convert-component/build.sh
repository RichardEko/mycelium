#!/usr/bin/env bash
# Regenerate the committed unit_convert_component.wasm fixture.
# Requires: rustup target add wasm32-wasip2
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip2
cp target/wasm32-wasip2/release/unit_convert_component.wasm ../unit_convert_component.wasm
echo "wrote ../unit_convert_component.wasm ($(wc -c < ../unit_convert_component.wasm) bytes)"
