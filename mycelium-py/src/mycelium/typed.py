"""
mycelium.typed — typed (schema-validated) calls to prompt skills on the mesh.

Wraps :meth:`~mycelium.prompt_skill.PromptSkillClient.call` with a
pydantic-validated output contract and a validation-feedback retry loop:
the skill's output is scanned for the first balanced JSON object (LLMs wrap
JSON in prose), validated against the caller's model, and on failure the call
is retried with the validation error handed back to the skill via
``context["validation_feedback"]``.

Scope note: this helper is for skills called *through the mesh* (the gateway's
``/gateway/llm/call`` path, where the provider is resolved per call). When your
code talks to an LLM provider directly, use the adopted Tier-1 libraries
instead — `Instructor <https://python.useinstructor.com/>`_ or
`Pydantic AI <https://ai.pydantic.dev/>`_ (``docs/plans/mycelium-reason.md``).

pydantic is an optional dependency: ``pip install "mycelium-py[typed]"``.

Example::

    from pydantic import BaseModel
    from mycelium import PromptSkillClient, call_typed

    class Match(BaseModel):
        lot: str
        pantry: str

    async def main():
        async with PromptSkillClient("127.0.0.1", 8101) as client:
            match = await call_typed(
                client, "llm", "fable-mini",
                '{"lot": "orchard-7", "pantry": "north"}', Match,
            )
            print(match.lot, "→", match.pantry)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pydantic import BaseModel

    from .prompt_skill import PromptSkillClient


class TypedCallError(Exception):
    """Raised when a typed call exhausts its retries without valid output.

    :attr:`last_output` is the skill's final raw output (``None`` when no JSON
    object was found in it, it is still the full output string);
    :attr:`last_error` is the final parse/validation error message.
    """

    def __init__(self, message: str, *, last_output: str, last_error: str) -> None:
        super().__init__(message)
        self.last_output = last_output
        self.last_error = last_error


def _extract_json_object(text: str) -> Optional[str]:
    """Return the first balanced ``{…}`` object in ``text``, or ``None``.

    A small brace scanner rather than a regex: it tracks JSON string literals
    (including escapes) so braces inside strings don't unbalance the scan —
    both LLMs and the EchoBackend wrap JSON in surrounding prose.
    """
    depth = 0
    start = -1
    in_string = False
    escaped = False
    for i, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if depth > 0 and ch == '"':
            in_string = True
        elif ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}" and depth > 0:
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


async def call_typed(
    client: "PromptSkillClient",
    ns: str,
    name: str,
    input: str,
    model: "type[BaseModel]",
    *,
    context: Optional[dict[str, Any]] = None,
    retries: int = 2,
    timeout_ms: int = 30_000,
) -> "BaseModel":
    """Invoke a prompt skill and validate its output against a pydantic model.

    Calls skill ``(ns, name)`` via the gateway, extracts the first balanced
    JSON object from the output, and validates it with
    ``model.model_validate_json``. On a parse or validation failure the call
    is retried (up to ``retries`` more times) with the error appended to the
    skill context as ``validation_feedback`` — templates that render
    ``{{validation_feedback}}`` let the model self-correct.

    :param client:     A :class:`~mycelium.prompt_skill.PromptSkillClient`.
    :param ns:         Capability namespace (e.g. ``"llm"``).
    :param name:       Capability name (e.g. the model id).
    :param input:      The ``{{input}}`` value rendered into the template.
    :param model:      pydantic model class the output must validate against.
    :param context:    Optional extra ``{{variable}}`` substitutions.
    :param retries:    Additional attempts after the first failure.
    :param timeout_ms: Per-call RPC timeout in milliseconds.
    :raises TypedCallError: After ``1 + retries`` failed attempts, carrying the
        last raw output and validation error.
    :raises ImportError: When pydantic is not installed
        (``pip install "mycelium-py[typed]"``).
    """
    try:
        from pydantic import ValidationError
    except ImportError as e:  # pragma: no cover - environment-dependent
        raise ImportError(
            "call_typed requires pydantic; install it with: pip install 'mycelium-py[typed]'"
        ) from e

    attempt_context: dict[str, Any] = dict(context or {})
    last_output = ""
    last_error = ""
    for _attempt in range(1 + retries):
        result = await client.call(ns, name, input, context=attempt_context, timeout_ms=timeout_ms)
        last_output = str(result.get("output", ""))
        candidate = _extract_json_object(last_output)
        if candidate is None:
            last_error = "no JSON object found in the skill output"
        else:
            try:
                return model.model_validate_json(candidate)
            except ValidationError as err:
                last_error = str(err)
        attempt_context = dict(context or {})
        attempt_context["validation_feedback"] = (
            f"The previous output failed validation: {last_error}. "
            f"Respond with only a JSON object matching the {model.__name__} schema."
        )
    raise TypedCallError(
        f"typed call to {ns}/{name} failed validation after {1 + retries} attempt(s): {last_error}",
        last_output=last_output,
        last_error=last_error,
    )
