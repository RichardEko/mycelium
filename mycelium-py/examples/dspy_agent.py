"""
Minimal DSPy-style agent that participates in a Mycelium mesh.

Advertises a "reasoning/planner" capability, resolves available tools,
and handles incoming "plan.request" signals.

Prerequisites:
    # Start a Mycelium node (any example)
    MOCK_LLM=1 cargo run --example llm_agent

    # Install SDK
    pip install -e mycelium-py

    # Run this script
    python mycelium-py/examples/dspy_agent.py
"""

import asyncio
import base64
import json
import sys

from mycelium import MyceliumAgent


async def main() -> None:
    # Connect to the locally-running Mycelium node's HTTP gateway.
    # In production, the gateway port comes from config / service discovery.
    agent = MyceliumAgent("127.0.0.1", port=8100)

    try:
        health = agent.health()
        print(f"[mesh] connected — node_id={health['node_id']}")
    except Exception as e:
        print(f"[error] cannot reach gateway: {e}", file=sys.stderr)
        return

    # Advertise this Python agent's capability so Rust agents can discover it.
    # Only the "orchestrator" role is permitted to resolve it.
    with agent.advertise_capability(
        "reasoning", "planner",
        interval_secs=30,
        attributes={"lang": "python", "framework": "dspy"},
        authorized_callers=["orchestrator"],
    ) as _handle:
        print("[mesh] advertised reasoning/planner (authorized_callers=['orchestrator'])")

        # Discover what tools the mesh currently offers.
        # Pass caller_id so capabilities with authorized_callers filters apply.
        tools = agent.resolve_capability("compute", "cpu", caller_id="planner")
        print(f"[mesh] resolved {len(tools)} compute/cpu provider(s)")
        for t in tools:
            print(f"       └─ {t['node_id']}  attrs={t['attributes']}")

        # Check demand pressure for LLM inference.
        status = agent.demand("llm", "inference")
        print(f"[mesh] llm/inference demand_pressure={status.demand_pressure:.2f} "
              f"(providers={status.providers}, requirers={status.requirers})")

        # Emit a signal to the mesh to announce readiness.
        agent.emit("planner.ready", json.dumps({"lang": "python"}).encode(), scope="system")
        print("[mesh] emitted planner.ready")

        # Listen for incoming planning requests.
        print("[mesh] listening for plan.request signals — Ctrl-C to stop")
        try:
            async for sig in agent.on_signal("plan.request"):
                try:
                    req = json.loads(sig.payload)
                except Exception:
                    req = {"raw": base64.b64encode(sig.payload).decode()}
                print(f"[signal] plan.request from {sig.sender}: {req}")

                # Echo a response back via emit (real agent would call DSPy here)
                response = json.dumps({"plan": ["step1", "step2"], "source": "python-dspy"})
                agent.emit("plan.response", response.encode(),
                           scope=f"node:{sig.sender}")
        except KeyboardInterrupt:
            print("\n[mesh] shutting down")


if __name__ == "__main__":
    asyncio.run(main())
