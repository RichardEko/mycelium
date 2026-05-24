## Mycelium — convenience targets

COMPOSE               = docker compose -f tests/integration/docker-compose.test.yml
COMPOSE_LLM           = docker compose -f docker/docker-compose.yml
COMPOSE_LLM_DEMO      = docker compose -f docker/docker-compose.llm-agent.yml
COMPOSE_THREE_NODE    = docker compose -f docker/docker-compose.three-node-test.yml

.PHONY: build test test-clean test-llm-demo test-llm-agent test-three-node llm-agent-interactive help

## test — build the cluster and run all integration scenarios
test:
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE) up -d --build
	@$(COMPOSE) logs -f runner & \
	EXIT=$$(docker wait mycelium-test-runner); \
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true; \
	exit $$EXIT

## test-clean — tear down the test cluster and remove volumes
test-clean:
	$(COMPOSE) down -v --remove-orphans

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

## llm-agent-interactive — start the llm_agent demo with real Ollama
## Open http://localhost:8100 for the mesh control UI.
llm-agent-interactive:
	MOCK_LLM=0 $(COMPOSE_LLM_DEMO) up --build llm-agent

## build — compile the library and the demo binary
build:
	cargo build --lib
	cargo build --example three_node_demo

## help
help:
	@grep -E '^##' Makefile | sed 's/^## //'
