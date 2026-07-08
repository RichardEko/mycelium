#!/usr/bin/env python3
"""
Rung 6 of the LangGraph example ladder (docs/plans/mycelium-reason-examples.md):
the **deploy/reheal flagship** — echo variant.

What this demo proves, end to end and deterministically:

    A LangGraph graph's *model dependency follows it across a node failure.*

A graph is checkpointed on node A (via ``MyceliumCheckpointSaver`` → A's gateway) and
interrupted before its LLM step. Node A — the only node serving the model — is then
**killed**. Node B, which did not serve the model at start, has *rehealed* it: its reheal
task declared the demand (``require_model`` → gossiped ``req/``), fetched the model
artifact over the mesh (a real cross-node ``MeshBlobStore`` fetch with SHA-256 verify),
and **bridged** it into a live ``serve_model`` skill. The thread resumes via B's gateway,
its routed LLM call lands on B, and the graph finishes — the model followed the thread.

The echo-fixture honesty caveat
-------------------------------
The "model artifact" is a tiny content-addressed blob and "serving" it is
``serve_model(EchoBackend)`` — the output is ``echo: {input}``, wasmtime-free and
deterministic (so this runs in CI). What is REAL is the seam: require_model → gossiped
demand → mesh blob fetch + verify → the serve_model bridge → routed resume. The showcase
variant (a later PR) streams actual GGUF weights via ``model_deploy``'s BlobRuntime and
serves them through local Ollama — same seam, real bytes. This demo proves the wiring, it
does not pretend the blob is a neural network.

The honest ordering (no faked instantaneity)
---------------------------------------------
Both handoffs wait for eventual consistency to converge *before* the kill, and both waits
are bounded structural polls (never fixed sleeps as the mechanism):

  1. The checkpoint replicates A → B (gossip) before A is killed.
  2. B's reheal task fetches the artifact from A (mesh) while A is still alive, and
     write-back caches it locally — so B keeps serving after A dies.

Run it directly (it manages its own two-node mesh, model_deploy-style):

    cargo build -p mycelium-reason --features llm,gateway --example reheal_node
    python examples/langgraph/06_deploy_reheal.py

Exits 0 after printing ``FLAGSHIP OK``; exits 1 on any assertion failure (dumping node
logs). Kills both nodes on every exit path.
"""

from __future__ import annotations

import atexit
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

import httpx
from typing_extensions import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph_checkpoint_mycelium import MyceliumCheckpointSaver

HOST = "127.0.0.1"
MODEL = "reheal-demo"

# Node A serves the model; node B reheals it. Distinct gossip + HTTP ports so both run on
# one host; B bootstraps off A.
#
# SEAM NOTE (a real finding — see the plan's rung-6 notes): a killed node lingers in a
# peer's capability view for the ~90s pheromone-freshness window (it simply stops
# refreshing; there is no instant tombstone), and the InferenceRouter ranks equal-load
# providers by ascending node-id. If the SURVIVING node had the higher gossip id, every
# post-kill route would try the dead A first and eat the 30s per-attempt RPC timeout
# before failing over. So the survivor B is given the LOWER gossip bind port (7301) — it
# ranks itself first and routes land on it immediately. The HTTP ports keep the plan's
# A=8301 / B=8302 assignment (those are what the driver + narrative speak to). This is a
# deliberate deviation from the plan's literal BIND 7301(A)/7302(B), for robustness.
A_BIND, A_HTTP = 7302, 8301
B_BIND, B_HTTP = 7301, 8302

# Generous CI-safe deadlines for the structural polls (kill/gossip races are real).
HEALTH_TIMEOUT = 60.0
CONVERGE_TIMEOUT = 60.0
REHEAL_TIMEOUT = 60.0

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE_BIN = REPO_ROOT / "target" / "debug" / "examples" / "reheal_node"

# Every node we spawn, so the atexit handler kills all of them even on failure.
_procs: list[subprocess.Popen] = []
_tmpdirs: list[tempfile.TemporaryDirectory] = []


def _cleanup() -> None:
    for p in _procs:
        if p.poll() is None:
            p.terminate()
    deadline = time.monotonic() + 5.0
    for p in _procs:
        try:
            p.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            p.kill()
    for d in _tmpdirs:
        try:
            d.cleanup()
        except OSError:
            pass


atexit.register(_cleanup)


def _dump_logs() -> None:
    for name, proc in (("A", _procs[0] if _procs else None), ("B", _procs[1] if len(_procs) > 1 else None)):
        if proc is None:
            continue
        log = getattr(proc, "_log_path", None)
        if log and Path(log).exists():
            print(f"\n───── node {name} log ({log}) ─────", file=sys.stderr)
            print(Path(log).read_text(), file=sys.stderr)


def start_node(*, bind: int, http: int, role_env: dict[str, str], name: str) -> subprocess.Popen:
    """Start a reheal_node subprocess with its own blob dir; log to a temp file."""
    d = tempfile.TemporaryDirectory(prefix=f"reheal-{name}-")
    _tmpdirs.append(d)
    env = {
        **os.environ,
        "BIND_PORT": str(bind),
        "HTTP_PORT": str(http),
        "BLOB_DIR": d.name,
        "MODEL": MODEL,
        **role_env,
    }
    log_path = Path(d.name) / f"node-{name}.log"
    log_fh = open(log_path, "w")
    proc = subprocess.Popen(
        [str(NODE_BIN)], env=env, stdout=log_fh, stderr=subprocess.STDOUT
    )
    proc._log_path = str(log_path)  # type: ignore[attr-defined]
    _procs.append(proc)
    return proc


def wait_healthy(http: int, timeout: float) -> None:
    """Poll /health until the gateway answers, bounded — never a fixed sleep."""
    deadline = time.monotonic() + timeout
    url = f"http://{HOST}:{http}/health"
    while True:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"gateway on {http} never became healthy")
        time.sleep(0.25)


def wait_log(proc: subprocess.Popen, needle: str, timeout: float) -> None:
    """Poll a node's stdout log until it contains ``needle`` — a structural signal
    from the node itself (no mesh routing, so it is safe while every node is alive)."""
    log = Path(proc._log_path)  # type: ignore[attr-defined]
    deadline = time.monotonic() + timeout
    while True:
        if log.exists() and needle in log.read_text():
            return
        assert time.monotonic() < deadline, f"log never showed {needle!r}"
        time.sleep(0.25)


# The router's per-attempt RPC timeout is 30s; a route that has to fail over past a
# just-departed provider can take that long, so client-side budgets sit above it.
ROUTE_TIMEOUT = 45.0


def route_once(http: int, prompt: str) -> Optional[httpx.Response]:
    """One sync POST to a node's /gateway/reason/route. None on connection error
    (the node may be mid-kill)."""
    try:
        return httpx.post(
            f"http://{HOST}:{http}/gateway/reason/route",
            json={"model": MODEL, "input": prompt},
            timeout=ROUTE_TIMEOUT,
        )
    except httpx.HTTPError:
        return None


# The speak node reads the *currently-live* gateway port from a mutable holder, so the
# same graph definition routes to A before the kill and B after it. graph.invoke is sync,
# and ReasonClient is async — so the node does a direct sync httpx.post instead.
class Port:
    value = A_HTTP


class State(TypedDict):
    prompt: str
    said: list


def build() -> StateGraph:
    def speak(state: State) -> dict:
        resp = httpx.post(
            f"http://{HOST}:{Port.value}/gateway/reason/route",
            json={"model": MODEL, "input": state["prompt"]},
            timeout=ROUTE_TIMEOUT,
        )
        resp.raise_for_status()
        return {"said": state["said"] + [resp.json()["output"]]}

    builder = StateGraph(State)
    builder.add_node("speak", speak)
    builder.add_edge(START, "speak")
    builder.add_edge("speak", END)
    return builder


def main() -> int:
    if not NODE_BIN.exists():
        print(
            f"reheal_node binary not found at {NODE_BIN}\n"
            "Build it first:\n"
            "  cargo build -p mycelium-reason --features llm,gateway --example reheal_node",
            file=sys.stderr,
        )
        return 1

    # ── 1–2. Start the two-node mesh: A serves + publishes; B reheals. ─────────────
    proc_a = start_node(bind=A_BIND, http=A_HTTP, name="a", role_env={"SERVE_MODEL": "1"})
    proc_b = start_node(
        bind=B_BIND,
        http=B_HTTP,
        name="b",
        role_env={"REHEAL": "1", "BOOTSTRAP": f"{HOST}:{A_BIND}"},
    )
    wait_healthy(A_HTTP, HEALTH_TIMEOUT)
    wait_healthy(B_HTTP, HEALTH_TIMEOUT)
    print("✓ mesh up: A serves reheal-demo, B will reheal it")

    prompt = "hello from the flagship"
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    # ── 3–4. Run the graph on A to the interrupt (before speak); checkpoint on A. ──
    Port.value = A_HTTP
    saver_a = MyceliumCheckpointSaver(HOST, A_HTTP)
    graph_a = build().compile(checkpointer=saver_a, interrupt_before=["speak"])
    graph_a.invoke({"prompt": prompt, "said": []}, config)
    head_a = saver_a.get_tuple(config)
    assert head_a is not None, "no checkpoint written on A"
    expected_id = head_a.checkpoint["id"]
    saver_a.close()
    print("✓ checkpointed on A (before speak)")

    # ── 5. Wait for the checkpoint to gossip A → B BEFORE killing A (honest). ──────
    saver_b = MyceliumCheckpointSaver(HOST, B_HTTP)
    deadline = time.monotonic() + CONVERGE_TIMEOUT
    while True:
        head_b = saver_b.get_tuple(config)
        if head_b is not None and head_b.checkpoint["id"] == expected_id:
            break
        assert time.monotonic() < deadline, "checkpoint never gossiped to B"
        time.sleep(0.25)
    print("✓ checkpoint gossiped to B")

    # ── Honest ordering, part 2: B must fetch the artifact from A over the mesh
    #    while A is still ALIVE (once A dies A cannot serve the blob). B's reheal task
    #    runs from B's startup and prints a marker when it has fetched + bridged the
    #    model — a structural signal straight from the node, no routing needed (routing
    #    to a live remote provider is not what we are testing here). ─────────────────
    wait_log(proc_b, "installed from mesh", REHEAL_TIMEOUT)
    print("✓ B fetched + bridged the model from the mesh (A still alive)")

    # ── 6. Kill node A gracefully. Its RAII skill handle retracts on shutdown, so B's
    #    routing view converges on B as the sole provider. ──────────────────────────
    proc_a.terminate()
    try:
        proc_a.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        proc_a.kill()
        proc_a.wait(timeout=5.0)
    print("✓ node A down")

    # ── 7. Now B is the only provider: a route to B lands on B (not the dead A). The
    #    generous per-request budget absorbs a failover past A if its retraction has
    #    not yet gossiped in — bounded, structural, never a fixed sleep. ────────────
    deadline = time.monotonic() + REHEAL_TIMEOUT
    b_node = f"{HOST}:{B_BIND}"
    while True:
        resp = route_once(B_HTTP, "ping")
        if resp is not None and resp.status_code == 200 and resp.json()["provider"] == b_node:
            break
        assert time.monotonic() < deadline, "B never became the sole live provider"
        time.sleep(0.25)
    print(f"✓ model rehealed on B — routes land on {b_node} (A is dead)")

    # ── 8. Resume on B: point the speak node at B and invoke(None, config). ────────
    Port.value = B_HTTP
    saver_b_resume = MyceliumCheckpointSaver(HOST, B_HTTP)
    graph_b = build().compile(checkpointer=saver_b_resume, interrupt_before=["speak"])
    final = graph_b.invoke(None, config)
    saver_b.close()
    saver_b_resume.close()

    assert final is not None and final.get("said"), "resume produced no output"
    said = final["said"]
    # The echo backend renders `echo: {input}` — the routed call ran and carried the prompt.
    assert any(prompt in line for line in said), (
        f"resumed output does not echo the prompt: {said!r}"
    )
    print(f"✓ resumed on B — routed inference produced: {said[-1]!r}")

    # ── 9. Narrate: where did the routed call land, and B's reheal trace. ──────────
    print(f"  routed inference landed on provider {b_node} (node B — A is dead)")
    try:
        trace = httpx.get(f"http://{HOST}:{B_HTTP}/gateway/reason/trace/reheal-{MODEL}", timeout=5.0)
        for line in trace.json().get("narrative", []):
            print(f"  trace: {line}")
    except httpx.HTTPError:
        pass

    # ── 10. Done. ─────────────────────────────────────────────────────────────────
    print("FLAGSHIP OK")
    return 0


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    except Exception as e:  # noqa: BLE001 — a driver: surface any failure, dump logs, exit 1
        print(f"flagship FAILED: {e}", file=sys.stderr)
        _dump_logs()
        code = 1
    finally:
        _cleanup()
    sys.exit(code)
