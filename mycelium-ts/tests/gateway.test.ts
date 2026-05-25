/**
 * Integration tests for the Mycelium TypeScript SDK.
 *
 * Requires a live Mycelium node. Set MYCELIUM_TEST_HOST and MYCELIUM_TEST_PORT
 * to point at it. All tests are skipped when those variables are absent.
 *
 * Start a node with:
 *   cargo run --example three_node_demo
 *   # or: MYCELIUM_ROLE=node cargo run --example three_node_demo
 *
 * Run:
 *   MYCELIUM_TEST_HOST=127.0.0.1 MYCELIUM_TEST_PORT=8300 npm test
 */

import { MyceliumAgent } from "../src/agent";

const TEST_HOST = process.env.MYCELIUM_TEST_HOST;
const TEST_PORT = parseInt(process.env.MYCELIUM_TEST_PORT ?? "0", 10);

const describe_ = TEST_HOST ? describe : describe.skip;
const it_ = TEST_HOST ? it : it.skip;

function agent(): MyceliumAgent {
  return new MyceliumAgent(TEST_HOST!, TEST_PORT, 10_000);
}

describe_("MyceliumAgent — live node tests", () => {
  let a: MyceliumAgent;

  beforeEach(() => {
    a = agent();
  });

  // ── Introspection ──────────────────────────────────────────────────────────

  it_("health returns ok", async () => {
    const h = await a.health();
    expect(h.status).toBe("ok");
    expect(typeof h.node_id).toBe("string");
  });

  it_("stats returns an object", async () => {
    const s = await a.stats();
    expect(typeof s).toBe("object");
  });

  it_("nodeId returns a non-empty string", async () => {
    const id = await a.nodeId;
    expect(id.length).toBeGreaterThan(0);
    expect(id).toContain(":");
  });

  // ── KV store ───────────────────────────────────────────────────────────────

  it_("set and get round-trips bytes", async () => {
    const key = `test/ts/${Date.now()}`;
    const val = Buffer.from("hello from typescript");
    await a.set(key, val);
    const got = await a.get(key);
    expect(got).not.toBeNull();
    expect(got!.toString()).toBe("hello from typescript");
  });

  it_("get returns null for missing key", async () => {
    const got = await a.get(`test/ts/missing/${Date.now()}`);
    expect(got).toBeNull();
  });

  it_("delete tombstones a key", async () => {
    const key = `test/ts/del/${Date.now()}`;
    await a.set(key, Buffer.from("x"));
    await a.delete(key);
    const got = await a.get(key);
    expect(got).toBeNull();
  });

  it_("keys returns prefix-filtered list", async () => {
    const prefix = `test/ts/keys/${Date.now()}`;
    await a.set(`${prefix}/a`, Buffer.from("1"));
    await a.set(`${prefix}/b`, Buffer.from("2"));
    // Give gossip a moment to stabilise.
    await new Promise((r) => setTimeout(r, 100));
    const ks = await a.keys(`${prefix}/`);
    expect(ks.length).toBeGreaterThanOrEqual(2);
  });

  it_("set_quorum with min_acks=0 returns 0 immediately", async () => {
    const key = `test/ts/quorum/${Date.now()}`;
    const n = await a.setQuorum(key, Buffer.from("v"), 0);
    expect(n).toBe(0);
  });

  // ── Capability advertisement ───────────────────────────────────────────────

  it_("advertise_capability returns a handle with a non-empty handleId", async () => {
    const handle = await a.advertiseCapability("ts-test", "ping");
    expect(handle.handleId.length).toBeGreaterThan(0);
    await handle.drop();
  });

  it_("resolve_capability finds advertised capability", async () => {
    const handle = await a.advertiseCapability("ts-test", "resolve-test", {
      attributes: { lang: "typescript" },
    });
    await new Promise((r) => setTimeout(r, 100));
    const providers = await a.resolveCapability("ts-test", "resolve-test");
    expect(providers.length).toBeGreaterThan(0);
    await handle.drop();
  });

  it_("demand returns a DemandStatus", async () => {
    const d = await a.demand("ts-test", "demand-check");
    expect(typeof d.demandPressure).toBe("number");
    expect(d.ns).toBe("ts-test");
    expect(d.name).toBe("demand-check");
  });

  // ── Signal mesh ────────────────────────────────────────────────────────────

  it_("emit returns a boolean", async () => {
    const queued = await a.emit("ts-test-signal", Buffer.from("hello"), {
      scope: "system",
    });
    expect(typeof queued).toBe("boolean");
  });

  // ── RPC ────────────────────────────────────────────────────────────────────

  it_("rpcCall times out with non-existent target", async () => {
    await expect(
      a.rpcCall("127.0.0.1:1", "echo", Buffer.from("hi"), { timeoutSecs: 0.2 }),
    ).rejects.toMatchObject({ name: "TimeoutError" });
  });

  // ── Mailbox ────────────────────────────────────────────────────────────────

  it_("deliverEvent does not throw", async () => {
    const id = await a.nodeId;
    await expect(
      a.deliverEvent(id, "ts-test.task", Buffer.from("payload")),
    ).resolves.not.toThrow();
  });

  // ── Overlay ────────────────────────────────────────────────────────────────

  it_("consistent_set and consistent_get round-trip", async () => {
    const key = `test/ts/overlay/${Date.now()}`;
    await a.consistentSet(key, Buffer.from("consistent-val"));
    const got = await a.consistentGet(key);
    expect(got?.toString()).toBe("consistent-val");
  });

  it_("electLeader returns a string node ID", async () => {
    const group = `ts-test-elect-${Date.now()}`;
    const leader = await a.electLeader(group);
    expect(typeof leader).toBe("string");
    expect(leader.length).toBeGreaterThan(0);
  });

  it_("append and scanLog round-trip", async () => {
    const stream = `ts-test-log-${Date.now()}`;
    const hlc1 = await a.append(stream, Buffer.from("entry-1"));
    const hlc2 = await a.append(stream, Buffer.from("entry-2"));
    expect(hlc2).toBeGreaterThan(hlc1);

    const entries = await a.scanLog(stream);
    expect(entries.length).toBeGreaterThanOrEqual(2);
    const values = entries.map((e) => e.value.toString());
    expect(values).toContain("entry-1");
    expect(values).toContain("entry-2");
  });

  it_("compactLog does not throw", async () => {
    const stream = `ts-test-compact-${Date.now()}`;
    const hlc = await a.append(stream, Buffer.from("x"));
    await expect(a.compactLog(stream, hlc + 1n)).resolves.not.toThrow();
  });

  it_("emitReliable to non-existent target returns timeout", async () => {
    const result = await a.emitReliable("127.0.0.1:1", "ts-test.reliable", Buffer.alloc(0), {
      timeoutSecs: 0.3,
    });
    expect(result).toBe("timeout");
  });
});
