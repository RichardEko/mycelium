"""
Integration tests for the mycelium-py HTTP gateway client.

Requires a running Mycelium node with http_port configured.

Run with:
    # terminal 1 — start Mycelium node
    MOCK_LLM=1 cargo run --example llm_agent

    # terminal 2 — run tests (connecting to n-0's HTTP port 8100)
    cd mycelium-py
    pip install -e ".[dev]"
    pytest tests/ -v -k gateway
"""

import asyncio
import os
import pytest

from mycelium import MyceliumAgent, DemandStatus

# Default to n-0's HTTP port; override with MYCELIUM_TEST_PORT env var.
TEST_HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
TEST_PORT  = int(os.getenv("MYCELIUM_TEST_PORT", "8100"))


@pytest.fixture
def agent() -> MyceliumAgent:
    return MyceliumAgent(TEST_HOST, TEST_PORT)


class TestHealth:
    def test_health_responds(self, agent: MyceliumAgent) -> None:
        data = agent.health()
        assert data["status"] == "ok"
        assert "node_id" in data

    def test_stats_responds(self, agent: MyceliumAgent) -> None:
        data = agent.stats()
        assert "node_id" in data
        assert "store_entries" in data


class TestCapabilityGateway:
    def test_advertise_returns_handle_id(self, agent: MyceliumAgent) -> None:
        handle = agent.advertise_capability("test", "advertise-probe", interval_secs=5)
        assert handle.handle_id
        handle.drop()

    def test_context_manager_drops_on_exit(self, agent: MyceliumAgent) -> None:
        with agent.advertise_capability("test", "ctx-probe", interval_secs=5) as h:
            assert h.handle_id

    def test_resolve_finds_advertised_cap(self, agent: MyceliumAgent) -> None:
        import time
        with agent.advertise_capability("test", "resolve-probe", interval_secs=5):
            time.sleep(0.2)   # let the gossip settle
            providers = agent.resolve_capability("test", "resolve-probe")
            assert len(providers) >= 1
            p = providers[0]
            assert p["ns"] == "test"
            assert p["name"] == "resolve-probe"
            assert "node_id" in p

    def test_authorized_callers_restricts_resolve(self, agent: MyceliumAgent) -> None:
        import time
        with agent.advertise_capability(
            "test", "auth-probe",
            interval_secs=5,
            authorized_callers=["privileged-caller"],
        ):
            time.sleep(0.2)
            # Unrestricted — should NOT see this capability
            visible_unrestricted = agent.resolve_capability("test", "auth-probe")
            assert len(visible_unrestricted) == 0

            # Privileged caller — should see it
            visible_privileged = agent.resolve_capability(
                "test", "auth-probe", caller_id="privileged-caller"
            )
            assert len(visible_privileged) == 1

            # Different caller — should NOT see it
            visible_other = agent.resolve_capability(
                "test", "auth-probe", caller_id="unprivileged"
            )
            assert len(visible_other) == 0

    def test_drop_tombstones_capability(self, agent: MyceliumAgent) -> None:
        import time
        handle = agent.advertise_capability("test", "drop-probe", interval_secs=5)
        time.sleep(0.2)
        providers_before = agent.resolve_capability("test", "drop-probe")
        assert len(providers_before) >= 1

        handle.drop()
        # Allow time for the tombstone to gossip
        time.sleep(0.5)
        providers_after = agent.resolve_capability("test", "drop-probe")
        assert len(providers_after) == 0


class TestSignalGateway:
    def test_emit_returns_ok(self, agent: MyceliumAgent) -> None:
        ok = agent.emit("test.probe", b"hello")
        assert ok is True

    def test_emit_with_attributes(self, agent: MyceliumAgent) -> None:
        ok = agent.emit("test.probe", b'{"action":"ping"}', scope="system")
        assert ok is True

    @pytest.mark.asyncio
    async def test_on_signal_receives_emitted(self, agent: MyceliumAgent) -> None:
        received: list = []

        async def listener() -> None:
            async for sig in agent.on_signal("test.roundtrip"):
                received.append(sig)
                break

        # Start listener then emit after a brief delay
        listener_task = asyncio.create_task(listener())
        await asyncio.sleep(0.05)
        agent.emit("test.roundtrip", b"payload-data")
        await asyncio.wait_for(listener_task, timeout=5.0)

        assert len(received) == 1
        assert received[0].payload == b"payload-data"
        assert received[0].kind == "test.roundtrip"


class TestDemandGateway:
    def test_demand_returns_status(self, agent: MyceliumAgent) -> None:
        status = agent.demand("test", "demand-probe")
        assert isinstance(status, DemandStatus)
        assert status.ns == "test"
        assert status.name == "demand-probe"
        assert status.providers >= 0
        assert status.requirers >= 0
        assert isinstance(status.demand_pressure, float)

    def test_demand_pressure_reflects_providers(self, agent: MyceliumAgent) -> None:
        import time
        with agent.advertise_capability("test", "pressure-probe", interval_secs=5):
            time.sleep(0.2)
            status = agent.demand("test", "pressure-probe")
            assert status.providers >= 1
            assert status.demand_pressure == pytest.approx(0.0, abs=0.1)
