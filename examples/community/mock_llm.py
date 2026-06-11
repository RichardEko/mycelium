#!/usr/bin/env python3
"""Mock OpenAI-compatible chat endpoint for CI runs of the community demo.

Stands in for Ollama on http://localhost:11434/v1/chat/completions so the
full skill mesh — orchestrator tool-call rounds, skill→skill RPC, A2A —
can be exercised deterministically with no model download and no GPU.

Two jobs beyond canned responses:

1. **Content-type validation.** Every `messages[].content` must be a string
   (or null). Ollama enforces this and a skillrunner regression once sent
   tool results as raw JSON objects ("invalid message content type:
   map[string]interface {}"), breaking every skill→skill composition. The
   mock returns the same 400 so that regression fails CI loudly.

2. **Deterministic tool-call rounds.** When the request declares `tools`,
   the mock walks the orchestrator through its documented flow by counting
   tool-role messages already in the transcript:
       round 0 → call researcher        round 1 → call writer
       round 2+ → final article JSON
   Leaf skills (no `tools` in request) get a canned JSON completion that
   satisfies every leaf schema (researcher/writer/verifier).
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("MOCK_LLM_PORT", "11434"))

LEAF_REPLY = json.dumps({
    "findings": ["mock finding one", "mock finding two"],
    "sources":  ["mock-source"],
    "summary":  "mock summary",
    "title":    "CI Article",
    "article":  "Deterministic article body produced by the mock LLM.",
    "tldr":     "mock tl;dr",
    "verified": True,
})

FINAL_REPLY = json.dumps({
    "title":   "CI Article",
    "article": "Deterministic article body produced by the mock LLM.",
    "tldr":    "mock tl;dr",
})


def _tool_call(call_id: str, name: str, args: dict) -> dict:
    return {
        "id":       call_id,
        "type":     "function",
        "function": {"name": name, "arguments": json.dumps(args)},
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet
        pass

    def _json(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802
        if not self.path.endswith("/chat/completions"):
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            req = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid JSON body"})
            return

        messages = req.get("messages", [])
        # Ollama-faithful validation: content must be a string (or null).
        for m in messages:
            content = m.get("content")
            if content is not None and not isinstance(content, str):
                self._json(400, {
                    "error": "invalid message content type: "
                             f"{type(content).__name__} (must be string)",
                })
                return

        tools       = req.get("tools") or []
        tool_rounds = sum(1 for m in messages if m.get("role") == "tool")
        msg: dict
        if tools and tool_rounds == 0:
            msg = {"role": "assistant", "content": None, "tool_calls": [
                _tool_call("call_mock_1", "researcher",
                           {"topic": "ci smoke topic", "max_points": 2}),
            ]}
            finish = "tool_calls"
        elif tools and tool_rounds == 1:
            msg = {"role": "assistant", "content": None, "tool_calls": [
                _tool_call("call_mock_2", "writer", {
                    "topic":    "ci smoke topic",
                    "findings": ["mock finding one", "mock finding two"],
                    "style":    "technical",
                }),
            ]}
            finish = "tool_calls"
        elif tools:
            msg, finish = {"role": "assistant", "content": FINAL_REPLY}, "stop"
        else:
            msg, finish = {"role": "assistant", "content": LEAF_REPLY}, "stop"

        self._json(200, {
            "choices": [{"message": msg, "finish_reason": finish}],
            "usage":   {"total_tokens": 1},
        })


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"mock-llm: listening on 127.0.0.1:{PORT}", flush=True)
    server.serve_forever()
