"""
Integration tests for mycelium.reason.ReasonClient against a running reason
node (the `reason_node` example: EchoBackend + template ``{{input}}``, serving
``llm/{MODEL}`` — so a routed call's output is ``echo: {input}``).

Requires MYCELIUM_TEST_PORT (a running reason node); tests skip cleanly when it
is unset. The served model is ``MYCELIUM_TEST_MODEL`` (default ``fable-mini``).
"""

import os

import pytest

from mycelium import NoProviderError, ReasonClient

TEST_HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
TEST_PORT = os.getenv("MYCELIUM_TEST_PORT")
TEST_MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")

pytestmark = pytest.mark.skipif(
    TEST_PORT is None,
    reason="MYCELIUM_TEST_PORT not set — no reason node to test against",
)


@pytest.mark.asyncio
async def test_route_echoes_and_reports_provider() -> None:
    async with ReasonClient(TEST_HOST, int(TEST_PORT)) as reason:
        result = await reason.route(TEST_MODEL, "hello-mesh")
        assert "hello-mesh" in result["output"]
        # A provider answered, addressed as ip:port; first candidate → attempt 1.
        assert ":" in result["provider"]
        assert result["attempt"] == 1


@pytest.mark.asyncio
async def test_route_no_provider_raises() -> None:
    async with ReasonClient(TEST_HOST, int(TEST_PORT)) as reason:
        with pytest.raises(NoProviderError):
            await reason.route("no-such-model", "x")


@pytest.mark.asyncio
async def test_blob_put_get_roundtrip() -> None:
    async with ReasonClient(TEST_HOST, int(TEST_PORT)) as reason:
        payload = b"routed-blob-fixture\x00\x01\x02"
        blob_id = await reason.blob_put(payload)
        assert blob_id
        assert await reason.blob_get(blob_id) == payload


@pytest.mark.asyncio
async def test_trace_unknown_run_is_empty() -> None:
    async with ReasonClient(TEST_HOST, int(TEST_PORT)) as reason:
        trace = await reason.trace("nonexistent-run")
        assert trace["run_id"] == "nonexistent-run"
        assert trace["events"] == []
