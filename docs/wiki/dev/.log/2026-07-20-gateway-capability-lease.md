# 2026-07-20 — Gateway capability lease (bridged-advert liveness)

**Trigger:** scraper-fleet debugging. Worker w15 crashed *after* its scrape but *before*
retracting its `scrape/worker` advert. The advert never evaporated: the mesh's 3× freshness
window (`CapEntry::is_fresh`) watches the *refresher*, and `POST /gateway/capability/advertise`
spawns the refresh loop (`run_kv_persist_task`) inside the **sidecar node**, not the client.
The bridge silently turns a soft-state lease into a sidecar-lifetime lease — provider liveness
decoupled from refresher liveness. Any bridged agent (Python worker, LLM tool process) has this
failure mode; judged frequent enough to fix in the substrate.

**Fix (opt-in, wire-unchanged, HTTP-only):**
- `lease_secs` on the advertise body + `POST /gateway/capability/{handle_id}/heartbeat`.
- A per-handle watchdog (`tokio::sync::Notify`, no clock, no new lock) retracts through the
  same path as DELETE (map removal drops the cancel sender → persist task tombstones).
- `409` heartbeat on a lease-less handle; `404` on a retracted one (client should re-advertise).
- SDKs: `mycelium-ts` (`leaseSecs`, `CapabilityHandle.heartbeat()`), `mycelium-py`
  (`lease_secs`, `heartbeat()`/`aheartbeat()`).
- Tests: `agent::http::tests::test_gateway_cap_lease_{expires_without_heartbeat,heartbeat_keeps_alive}`
  (structural polling, no fixed-sleep assertions).

**Rule of thumb ingested to [operations.md](../operations.md):** a bridged advert whose real
owner is the *client process* must carry `lease_secs`; omit it only when the capability's
lifetime genuinely is the node's.
