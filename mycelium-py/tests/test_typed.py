"""
Integration tests for mycelium.typed.call_typed against a reason node's echo
skill (the `reason_node` example: EchoBackend + template ``{{input}}``, so a
call's output is ``echo: {input}`` — JSON wrapped in prose, exactly what the
brace scanner must handle).

Requires MYCELIUM_TEST_PORT (a running reason node); tests skip cleanly when
it is unset. The skill is ``llm/{MODEL}`` (default ``fable-mini``).
"""

import json
import os

import pytest

from mycelium import PromptSkillClient, TypedCallError, call_typed
from mycelium.typed import _extract_json_object

TEST_HOST = os.getenv("MYCELIUM_TEST_HOST", "127.0.0.1")
TEST_PORT = os.getenv("MYCELIUM_TEST_PORT")
TEST_MODEL = os.getenv("MYCELIUM_TEST_MODEL", "fable-mini")

pytestmark = pytest.mark.skipif(
    TEST_PORT is None,
    reason="MYCELIUM_TEST_PORT not set — no reason node to test against",
)

pydantic = pytest.importorskip("pydantic")


class Lot(pydantic.BaseModel):
    produce: str
    kilos: int


class _SpyClient:
    """Wraps PromptSkillClient.call to record the contexts actually sent."""

    def __init__(self, inner: PromptSkillClient) -> None:
        self._inner = inner
        self.contexts: list[dict] = []

    async def call(self, ns, name, input, context=None, timeout_ms=30_000):
        self.contexts.append(dict(context or {}))
        return await self._inner.call(ns, name, input, context=context, timeout_ms=timeout_ms)


class TestExtractJsonObject:
    def test_object_wrapped_in_prose(self) -> None:
        assert _extract_json_object('echo: {"a": 1} trailing') == '{"a": 1}'

    def test_nested_objects_and_braces_in_strings(self) -> None:
        text = 'noise {"a": {"b": "}{"}, "c": 2} more'
        assert _extract_json_object(text) == '{"a": {"b": "}{"}, "c": 2}'

    def test_no_object(self) -> None:
        assert _extract_json_object("just prose, no json") is None
        assert _extract_json_object("unbalanced { forever") is None


class TestCallTyped:
    @pytest.mark.asyncio
    async def test_happy_path_extracts_and_validates(self) -> None:
        async with PromptSkillClient(TEST_HOST, int(TEST_PORT)) as client:
            lot = await call_typed(
                client,
                "llm",
                TEST_MODEL,
                json.dumps({"produce": "apples", "kilos": 40}),
                Lot,
            )
            assert isinstance(lot, Lot)
            assert lot.produce == "apples"
            assert lot.kilos == 40

    @pytest.mark.asyncio
    async def test_failure_retries_with_feedback_then_raises(self) -> None:
        async with PromptSkillClient(TEST_HOST, int(TEST_PORT)) as client:
            spy = _SpyClient(client)
            # The echo output can never satisfy the schema (kilos is missing).
            with pytest.raises(TypedCallError) as exc_info:
                await call_typed(
                    spy,
                    "llm",
                    TEST_MODEL,
                    json.dumps({"produce": "apples"}),
                    Lot,
                    retries=2,
                )
            err = exc_info.value
            assert "apples" in err.last_output
            assert "kilos" in err.last_error

            # 1 initial + 2 retries, and every retry carried validation feedback.
            assert len(spy.contexts) == 3
            assert "validation_feedback" not in spy.contexts[0]
            for ctx in spy.contexts[1:]:
                assert "kilos" in ctx["validation_feedback"]
