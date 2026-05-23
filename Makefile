## Mycelium — convenience targets

COMPOSE      = docker compose -f tests/integration/docker-compose.test.yml
COMPOSE_LLM  = docker compose -f docker/docker-compose.yml

.PHONY: build test test-clean test-llm-demo help

## test — build the cluster and run all 7 unattended integration scenarios
test:
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from runner

## test-clean — tear down the test cluster and remove volumes
test-clean:
	$(COMPOSE) down -v --remove-orphans

## test-llm-demo — manual scenario 8: start the LLM demo cluster
## Requires Ollama installed locally (llama3.2 will be pulled on first run).
## Open http://localhost:8080 to start chatting.
test-llm-demo:
	$(COMPOSE_LLM) up --build

## build — compile the library and the demo binary
build:
	cargo build --lib
	cargo build --example three_node_demo

## help
help:
	@grep -E '^##' Makefile | sed 's/^## //'
