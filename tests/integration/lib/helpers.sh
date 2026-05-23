#!/usr/bin/env bash
# Sourced by every scenario script — do NOT execute directly.

# wait_for_health HOST HTTP_PORT [TIMEOUT_SECS]
wait_for_health() {
    local host="$1" port="$2" timeout="${3:-30}"
    local url="http://${host}:${port}/health"
    local i=0
    while [ "$i" -lt "$timeout" ]; do
        if curl -sf --max-time 3 "$url" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    echo "TIMEOUT: ${url} did not become healthy within ${timeout}s" >&2
    return 1
}

# poll_until TIMEOUT_SECS COMMAND [ARGS…]
# Returns 0 if COMMAND succeeds within TIMEOUT_SECS attempts (1 s apart).
poll_until() {
    local timeout="$1"; shift
    local i=0
    while [ "$i" -lt "$timeout" ]; do
        if "$@" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    echo "TIMEOUT: '$*' did not succeed within ${timeout}s" >&2
    return 1
}

# kv_get HOST KEY → raw response body, empty string on 404 / error
kv_get() {
    local host="$1" key="$2"
    curl -sf --max-time 5 "http://${host}:${NODE_HTTP_PORT:-8300}/kv/${key}" 2>/dev/null || echo ""
}

# kv_put HOST KEY VALUE
kv_put() {
    local host="$1" key="$2" value="$3"
    curl -sf --max-time 5 -X PUT -d "$value" \
        "http://${host}:${NODE_HTTP_PORT:-8300}/kv/${key}" > /dev/null
}

# kv_check HOST KEY EXPECTED_VALUE — used with poll_until
kv_check() {
    local host="$1" key="$2" expected="$3"
    local actual
    actual=$(kv_get "$host" "$key")
    [ "$actual" = "$expected" ]
}

# assert_eq ACTUAL EXPECTED [MSG]
assert_eq() {
    local actual="$1" expected="$2" msg="${3:-}"
    if [ "$actual" != "$expected" ]; then
        printf "FAIL%s: expected='%s' got='%s'\n" \
            "${msg:+: $msg}" "$expected" "$actual" >&2
        return 1
    fi
    return 0
}

# assert_ge ACTUAL MIN [MSG]  — numeric ≥
assert_ge() {
    local actual="$1" min="$2" msg="${3:-}"
    if [ "$actual" -lt "$min" ] 2>/dev/null; then
        printf "FAIL%s: expected>=%s, got %s\n" \
            "${msg:+: $msg}" "$min" "$actual" >&2
        return 1
    fi
    return 0
}

# mgmt_state — returns the JSON body from /api/state
mgmt_state() {
    curl -sf --max-time 5 "http://${MGMT_HOST:-mgmt}:${MGMT_HTTP_PORT:-8090}/api/state"
}
