## Mycelium — convenience targets

COMPOSE               = docker compose -f tests/integration/docker-compose.test.yml
COMPOSE_SCALE         = docker compose -f tests/integration/docker-compose.scale.yml
COMPOSE_RESILIENCE    = docker compose -f tests/integration/docker-compose.scale-resilience.yml
COMPOSE_SCALE_ENTRIES = docker compose -f tests/integration/docker-compose.scale-entries.yml
COMPOSE_LLM           = docker compose -f docker/docker-compose.yml
COMPOSE_LLM_DEMO      = docker compose -f docker/docker-compose.llm-agent.yml
COMPOSE_THREE_NODE    = docker compose -f docker/docker-compose.three-node-test.yml
COMPOSE_OVERLAY       = docker compose -f tests/overlay/docker-compose.test.yml

.PHONY: build check check-full test test-clean test-scale test-scale-clean test-scale-resilience test-scale-resilience-clean test-scale-entries test-scale-entries-clean test-llm-demo test-llm-agent test-three-node test-overlay llm-agent-interactive help

## test — build the cluster and run all integration scenarios
test:
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE) up -d --build
	@$(COMPOSE) logs -f runner & \
	EXIT=$$(docker wait mycelium-test-runner); \
	if [ "$$EXIT" != "0" ]; then \
	    echo "── runner failed: node logs (last 200 lines each, for CI diagnosis) ──"; \
	    $(COMPOSE) logs --tail 200 node-a node-b node-c mgmt 2>/dev/null || true; \
	fi; \
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-clean — tear down the test cluster and remove volumes
test-clean:
	$(COMPOSE) down -v --remove-orphans

## test-scale — 100-node cluster scale test (1 seed + 99 workers + mgmt + runner)
## Requires a warm Docker build cache (run `make test` first to prime it).
## Takes ~3 min: ~60 s cluster formation + 60 s gossip propagation window.
## Override SCALE_WORKERS to test at a different size: make test-scale SCALE_WORKERS=49
SCALE_WORKERS ?= 99
test-scale:
	$(COMPOSE_SCALE) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE_SCALE) up -d --build --scale worker=$(SCALE_WORKERS)
	@$(COMPOSE_SCALE) logs -f runner & \
	EXIT=$$(docker wait mycelium-scale-runner); \
	$(COMPOSE_SCALE) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-scale-clean — tear down the scale test cluster and remove volumes
test-scale-clean:
	$(COMPOSE_SCALE) down -v --remove-orphans

## test-scale-baseline — WS-B Phase 0: record the connection-ceiling "before" curve.
## Brings the scale cluster up at each worker count and captures seed ESTABLISHED,
## host conntrack, FORWARD-chain rule count, and seed /stats into
## tests/integration/baseline/scale-baseline.csv. Host-side (needs Docker socket +
## privilege for the --net=host VM probe). Long-running: one cluster up/down per point.
## Override the curve: make test-scale-baseline BASELINE_WORKERS="30 50 70 100"
BASELINE_WORKERS ?= 30 50 70 100
test-scale-baseline:
	BASELINE_WORKERS="$(BASELINE_WORKERS)" tests/integration/measure_scale_baseline.sh

## test-scale-resilience — crash/rejoin/anti-entropy/churn test (~22 nodes: 1 seed + 20 workers + mgmt)
## Tests: cluster formation, crash+recovery, late-joiner anti-entropy, and churn stability.
## Requires a warm Docker build cache and Docker socket access.  ~8 min on warm cache.
## Default is 20 workers — stays within the Docker bridge iptables connection limit so the
## Phase 3 late-joiner probe can establish a new TCP connection to seed (see CLAUDE.md §iptables).
## For higher scale (50+) switch the Docker network driver to macvlan or enable nftables first.
## Override: make test-scale-resilience RESILIENCE_WORKERS=50
RESILIENCE_WORKERS ?= 20
test-scale-resilience:
	$(COMPOSE_RESILIENCE) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE_RESILIENCE) up -d --build --scale worker=$(RESILIENCE_WORKERS)
	@$(COMPOSE_RESILIENCE) logs -f runner & \
	EXIT=$$(docker wait mycelium-resilience-runner); \
	$(COMPOSE_RESILIENCE) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-scale-resilience-clean — tear down the resilience test cluster and remove volumes
test-scale-resilience-clean:
	$(COMPOSE_RESILIENCE) down -v --remove-orphans

## test-scale-entries — entry-volume axis test (~30 nodes: 1 seed + 29 workers + mgmt)
## The 100-node test (test-scale) validates the node-count axis. This test
## validates the orthogonal entry-volume axis: load ENTRY_COUNT synthetic
## entries onto a 30-node cluster and measure convergence + anti-entropy
## sweep tail. 30 nodes deliberately stays well below the iptables ceiling
## so the runner can make new TCP connections throughout the test.
## Override examples:
##   make test-scale-entries ENTRY_COUNT=10000 ENTRY_BYTES=1024    # bytes-axis probe
##   make test-scale-entries ENTRY_COUNT=20000 WRITE_DELAY_MS=30   # sustained-rate sanity check (~10 min)
##   make test-scale-entries SCALE_ENTRIES_WORKERS=49              # wider cluster
SCALE_ENTRIES_WORKERS ?= 29
ENTRY_COUNT           ?= 5000
ENTRY_BYTES           ?= 512
WRITE_DELAY_MS        ?= 0
test-scale-entries:
	$(COMPOSE_SCALE_ENTRIES) down -v --remove-orphans 2>/dev/null || true
	ENTRY_COUNT=$(ENTRY_COUNT) ENTRY_BYTES=$(ENTRY_BYTES) WRITE_DELAY_MS=$(WRITE_DELAY_MS) \
	    $(COMPOSE_SCALE_ENTRIES) up -d --build --scale worker=$(SCALE_ENTRIES_WORKERS)
	@$(COMPOSE_SCALE_ENTRIES) logs -f runner & \
	EXIT=$$(docker wait mycelium-scale-entries-runner); \
	$(COMPOSE_SCALE_ENTRIES) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-scale-entries-clean — tear down the entry-volume test cluster and remove volumes
test-scale-entries-clean:
	$(COMPOSE_SCALE_ENTRIES) down -v --remove-orphans

## test-llm-demo — manual scenario: start the three_node_demo LLM cluster
## Requires Ollama installed locally. Open http://localhost:8080 to chat.
test-llm-demo:
	$(COMPOSE_LLM) up --build

## test-llm-agent — automated Docker test of the llm_agent example (MOCK_LLM=1)
## Builds the container, runs 6 scenarios, tears down. No Ollama needed.
test-llm-agent:
	$(COMPOSE_LLM_DEMO) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE_LLM_DEMO) up -d --build
	@$(COMPOSE_LLM_DEMO) logs -f runner & \
	EXIT=$$(docker wait mycelium-llm-agent-runner); \
	$(COMPOSE_LLM_DEMO) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-three-node — automated Docker test of three_node_demo with real Ollama
## Runs 4 scenarios: tool discovery, tool health, HTML UI, chat round-trip.
## Downloads llama3.2 (~2 GB) on first run; cached in the ollama-models volume.
test-three-node:
	$(COMPOSE_THREE_NODE) down --remove-orphans 2>/dev/null || true
	$(COMPOSE_THREE_NODE) up -d --build
	@$(COMPOSE_THREE_NODE) logs -f runner & \
	EXIT=$$(docker wait mycelium-three-node-runner); \
	$(COMPOSE_THREE_NODE) down --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-overlay — 3-node overlay cluster: task auction, leader election, shared log
## Builds Docker images, starts cluster, runs S11/S12/S13. ~3 min on warm cache.
test-overlay:
	$(COMPOSE_OVERLAY) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE_OVERLAY) up -d --build
	@$(COMPOSE_OVERLAY) logs -f runner & \
	EXIT=$$(docker wait mycelium-overlay-runner); \
	$(COMPOSE_OVERLAY) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## llm-agent-interactive — start the llm_agent demo with real Ollama
## Open http://localhost:8100 for the mesh control UI.
llm-agent-interactive:
	MOCK_LLM=0 $(COMPOSE_LLM_DEMO) up --build llm-agent

## build — compile the library and the demo binary
build:
	cargo build --lib
	cargo build --example three_node_demo

## check — the pre-push gate. Runs clippy across the feature matrix that CI enforces, in ONE
## command, so the feature-gated dead-code trap (an item live only under gateway/metrics is *dead*
## under --no-default-features) is caught locally instead of turning a push CI-red. The
## --no-default-features clippy is the trap-catcher — it lints the same gateway/metrics-off mycelium
## lib that CI's "Gateway-free build" and "WASM host" jobs compile — so this is fast (~3 min, no
## wasmtime). Run it before every push.
check:
	cargo clippy --lib --tests --features tls,metrics,a2a,llm -- -D warnings
	cargo clippy --lib --no-default-features -- -D warnings
	cargo clippy -p mycelium-core --lib --tests -- -D warnings

## check-full — check + the test suites + the (slow, wasmtime-heavy) wasm-host clippy. Mirrors the
## CI gate set; run before a release or when you have touched wasm-host / a feature-conditional path.
check-full: check
	cargo test  --lib --features tls,metrics,a2a,llm
	cargo test  --lib --no-default-features --features gateway
	cargo test  -p mycelium-core          # the substrate suite (codec/framing/hlc/store/swim) + the wire back-compat gate
	cargo clippy -p mycelium-wasm-host --all-targets -- -D warnings

## help
help:
	@grep -E '^##' Makefile | sed 's/^## //'
