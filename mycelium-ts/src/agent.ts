import { sseStream } from "./sse";
import {
  CapabilityHandle,
  DemandStatus,
  LockGuard,
  LogEntry,
  MailboxEvent,
  RpcRequest,
  Signal,
} from "./types";

function b64(buf: Buffer | Uint8Array): string {
  return Buffer.from(buf).toString("base64");
}

function fromb64(s: string): Buffer {
  return Buffer.from(s, "base64");
}

/**
 * Connects to a running Rust Mycelium node over loopback HTTP.
 *
 * No native extension — the HTTP gateway sidecar adds ~1 ms per call,
 * invisible next to LLM inference latency.
 */
export class MyceliumAgent {
  private readonly base: string;
  private readonly timeout: number;
  private _nodeId: string | null = null;

  /**
   * @param host    Gateway host (default "127.0.0.1")
   * @param port    HTTP port the Mycelium node listens on (default 7946)
   * @param timeout Default request timeout in milliseconds (default 30_000)
   */
  constructor(
    host = "127.0.0.1",
    port = 7946,
    timeout = 30_000,
  ) {
    this.base = `http://${host}:${port}`;
    this.timeout = timeout;
  }

  // ── Internals ─────────────────────────────────────────────────────────────

  private async _get(path: string, params?: Record<string, string>): Promise<unknown> {
    const url = new URL(path, this.base);
    if (params) {
      for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    }
    const resp = await fetch(url.toString(), {
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!resp.ok) throw new Error(`GET ${path} failed: ${resp.status}`);
    return resp.json();
  }

  private async _post(path: string, body: unknown): Promise<unknown> {
    const resp = await fetch(`${this.base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`POST ${path} failed: ${resp.status} ${text}`);
    }
    return resp.json();
  }

  private async _delete(path: string): Promise<void> {
    const resp = await fetch(`${this.base}${path}`, {
      method: "DELETE",
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!resp.ok) throw new Error(`DELETE ${path} failed: ${resp.status}`);
  }

  private _sseUrl(path: string, params?: Record<string, string>): string {
    const url = new URL(path, this.base);
    if (params) {
      for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
    }
    return url.toString();
  }

  // ── Introspection ─────────────────────────────────────────────────────────

  /** Returns `{ status: "ok", node_id: "..." }`. */
  async health(): Promise<{ status: string; node_id: string }> {
    return this._get("/health") as Promise<{ status: string; node_id: string }>;
  }

  /** Returns a snapshot of store statistics. */
  async stats(): Promise<Record<string, unknown>> {
    return this._get("/stats") as Promise<Record<string, unknown>>;
  }

  /** This node's `"ip:port"` identifier (cached after first call). */
  get nodeId(): Promise<string> {
    if (this._nodeId) return Promise.resolve(this._nodeId);
    return this.health().then((h) => {
      this._nodeId = h.node_id;
      return h.node_id;
    });
  }

  // ── Capability advertisement ──────────────────────────────────────────────

  /**
   * Advertises a capability on the mesh. Re-asserted every `intervalSecs` so
   * late joiners discover it. Returns a `CapabilityHandle`; call `.drop()` or
   * use `await using` to retract.
   */
  async advertiseCapability(
    ns: string,
    name: string,
    options: {
      intervalSecs?: number;
      attributes?: Record<string, unknown>;
      authorizedCallers?: string[];
    } = {},
  ): Promise<CapabilityHandle> {
    const data = await this._post("/gateway/capability/advertise", {
      ns,
      name,
      interval_secs: options.intervalSecs ?? 30,
      attributes: options.attributes ?? {},
      authorized_callers: options.authorizedCallers ?? [],
    }) as { handle_id: string };
    const handleId = data.handle_id;
    return new CapabilityHandle(handleId, async () => {
      await this._delete(`/gateway/capability/${handleId}`);
    });
  }

  /**
   * Returns all live providers matching `(ns, name)`.
   * Pass `callerId` to respect `authorized_callers` restrictions.
   */
  async resolveCapability(
    ns: string,
    name: string,
    options: { callerId?: string } = {},
  ): Promise<Array<Record<string, unknown>>> {
    const params: Record<string, string> = { ns, name };
    if (options.callerId) params.caller_id = options.callerId;
    return this._get("/gateway/capability/resolve", params) as Promise<Array<Record<string, unknown>>>;
  }

  /**
   * Returns demand pressure: `demandPressure > 1.0` signals a supply gap.
   */
  async demand(ns: string, name: string): Promise<DemandStatus> {
    const raw = await this._get("/gateway/demand", { ns, name }) as {
      ns: string; name: string; providers: number; requirers: number; demand_pressure: number;
    };
    return {
      ns: raw.ns,
      name: raw.name,
      providers: raw.providers,
      requirers: raw.requirers,
      demandPressure: raw.demand_pressure,
    };
  }

  // ── Signal mesh ───────────────────────────────────────────────────────────

  /**
   * Fires a signal into the mesh.
   * @param scope `"system"` (default), `"group:NAME"`, or `"node:IP:PORT"`
   * @returns `true` if queued for gossip; `false` if the shard was full (local delivery still occurred).
   */
  async emit(
    kind: string,
    payload: Buffer | Uint8Array = Buffer.alloc(0),
    options: { scope?: string } = {},
  ): Promise<boolean> {
    const data = await this._post("/gateway/signal/emit", {
      kind,
      payload_b64: b64(payload),
      scope: options.scope ?? "system",
    }) as { queued: boolean };
    return data.queued;
  }

  /**
   * Async generator yielding admitted signals of `kind` as SSE events.
   */
  async *onSignal(kind: string): AsyncGenerator<Signal> {
    const url = this._sseUrl(`/gateway/signal/sse/${encodeURIComponent(kind)}`);
    yield* sseStream<Signal>(url, (data) => {
      const raw = JSON.parse(data) as {
        kind: string; sender: string; payload_b64: string; nonce: string;
      };
      return {
        kind: raw.kind,
        sender: raw.sender,
        payload: fromb64(raw.payload_b64),
        nonce: BigInt(raw.nonce),
      };
    });
  }

  // ── KV store ──────────────────────────────────────────────────────────────

  /** Reads a key, returns `null` if absent or tombstoned. */
  async get(key: string): Promise<Buffer | null> {
    const data = await this._get("/gateway/kv", { key }) as { value_b64: string | null };
    return data.value_b64 !== null ? fromb64(data.value_b64) : null;
  }

  /** Writes a key and queues it for gossip. */
  async set(key: string, value: Buffer | Uint8Array): Promise<void> {
    await this._post("/gateway/kv", { key, value_b64: b64(value) });
  }

  /** Tombstones a key and queues for gossip. */
  async delete(key: string): Promise<void> {
    const resp = await fetch(`${this.base}/gateway/kv?key=${encodeURIComponent(key)}`, {
      method: "DELETE",
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!resp.ok) throw new Error(`DELETE /gateway/kv failed: ${resp.status}`);
  }

  /** Lists live keys with an optional prefix filter. */
  async keys(prefix = ""): Promise<string[]> {
    const data = await this._get("/gateway/kv/keys", { prefix }) as { keys: string[] };
    return data.keys;
  }

  /** Returns all live key-value pairs under `prefix`. */
  async scanPrefix(prefix: string): Promise<Record<string, Buffer>> {
    const ks = await this.keys(prefix);
    const pairs = await Promise.all(
      ks.map(async (k) => [k, await this.get(k)] as const),
    );
    return Object.fromEntries(
      pairs.filter(([, v]) => v !== null).map(([k, v]) => [k, v as Buffer]),
    );
  }

  /**
   * Writes `value` and waits for at least `minAcks` distinct peers to confirm.
   * Returns the confirmed peer count on success; throws `TimeoutError` on timeout.
   */
  async setQuorum(
    key: string,
    value: Buffer | Uint8Array,
    minAcks: number,
    options: { timeoutSecs?: number } = {},
  ): Promise<number> {
    const data = await this._post("/gateway/kv/quorum", {
      key,
      value_b64: b64(value),
      min_acks: minAcks,
      timeout_secs: options.timeoutSecs ?? 5,
    }) as { ok: boolean; acks_received: number; error?: string };
    if (!data.ok) {
      throw Object.assign(new Error(`set_quorum timeout (${data.acks_received} acks)`), {
        name: "TimeoutError",
        acksReceived: data.acks_received,
      });
    }
    return data.acks_received;
  }

  // ── RPC ───────────────────────────────────────────────────────────────────

  /**
   * Blocking point-to-point RPC call. Throws `TimeoutError` if no reply arrives.
   */
  async rpcCall(
    target: string,
    method: string,
    payload: Buffer | Uint8Array = Buffer.alloc(0),
    options: { timeoutSecs?: number } = {},
  ): Promise<Buffer> {
    const data = await this._post("/gateway/rpc/call", {
      target,
      kind: method,
      payload_b64: b64(payload),
      timeout_secs: options.timeoutSecs ?? 5,
    }) as { ok: boolean; result_b64?: string; error?: string };
    if (!data.ok) throw Object.assign(new Error("rpc_call timeout"), { name: "TimeoutError" });
    return fromb64(data.result_b64!);
  }

  /**
   * Async generator yielding incoming RPC requests of `kind`.
   * Call `rpcRespond` for each request to complete the round-trip.
   */
  async *rpcServe(kind: string): AsyncGenerator<RpcRequest> {
    const url = this._sseUrl(`/gateway/rpc/serve/${encodeURIComponent(kind)}`);
    yield* sseStream<RpcRequest>(url, (data) => {
      const raw = JSON.parse(data) as {
        kind: string; nonce_hex: string; sender: string; payload_b64: string;
      };
      return {
        kind: raw.kind,
        nonceHex: raw.nonce_hex,
        sender: raw.sender,
        payload: fromb64(raw.payload_b64),
      };
    });
  }

  /** Sends a reply to an in-flight RPC request. */
  async rpcRespond(request: RpcRequest, result: Buffer | Uint8Array = Buffer.alloc(0)): Promise<void> {
    await this._post("/gateway/rpc/respond", {
      nonce_hex: request.nonceHex,
      sender: request.sender,
      result_b64: b64(result),
    });
  }

  /**
   * Fan-out RPC to multiple targets; waits for at least `minOk` replies.
   * Throws `TimeoutError` if the threshold is not met.
   */
  async scatterGather(
    targets: string[],
    method: string,
    payload: Buffer | Uint8Array = Buffer.alloc(0),
    options: { minOk?: number; timeoutSecs?: number } = {},
  ): Promise<Array<{ sender: string; result: Buffer }>> {
    const data = await this._post("/gateway/scatter", {
      targets,
      kind: method,
      payload_b64: b64(payload),
      min_ok: options.minOk ?? targets.length,
      timeout_secs: options.timeoutSecs ?? 5,
    }) as { ok: boolean; replies?: Array<{ sender: string; result_b64: string }>; error?: string };
    if (!data.ok) throw Object.assign(new Error("scatter_gather timeout"), { name: "TimeoutError" });
    return (data.replies ?? []).map((r) => ({
      sender: r.sender,
      result: fromb64(r.result_b64),
    }));
  }

  // ── Mailbox ───────────────────────────────────────────────────────────────

  /**
   * Delivers a mailbox event to `target`'s mailbox.
   * At-least-once within TTL; gossiped to all peers.
   */
  async deliverEvent(target: string, kind: string, payload: Buffer | Uint8Array = Buffer.alloc(0)): Promise<void> {
    await this._post("/gateway/mailbox/deliver", {
      target,
      kind,
      payload_b64: b64(payload),
    });
  }

  /**
   * Streams events of `kind` addressed to this node.
   * Events are delivered in HLC-causal order and tombstoned on delivery.
   */
  async *mailbox(kind: string): AsyncGenerator<MailboxEvent> {
    const url = this._sseUrl(`/gateway/mailbox/${encodeURIComponent(kind)}`);
    yield* sseStream<MailboxEvent>(url, (data) => {
      const raw = JSON.parse(data) as {
        kind: string; sender: string; payload_b64: string;
      };
      return {
        kind: raw.kind,
        sender: raw.sender,
        payload: fromb64(raw.payload_b64),
      };
    });
  }

  // ── Consistency & Ordering Overlay ────────────────────────────────────────

  /**
   * Linearizable KV write: runs a consensus round before writing.
   * All nodes observe the same value even under concurrent writes.
   */
  async consistentSet(key: string, value: Buffer | Uint8Array): Promise<void> {
    await this._post("/gateway/overlay/consistent/set", {
      key,
      value_b64: b64(value),
    });
  }

  /** Linearizable KV read. */
  async consistentGet(key: string): Promise<Buffer | null> {
    const data = await this._get("/gateway/overlay/consistent/get", { key }) as {
      value_b64: string | null;
    };
    return data.value_b64 !== null ? fromb64(data.value_b64) : null;
  }

  /**
   * Acquires a named cluster lock via consensus.
   * Returns a `LockGuard`; use `await using` or call `.release()`.
   */
  async distributedLock(
    name: string,
    options: { ttlSecs?: number } = {},
  ): Promise<LockGuard> {
    const data = await this._post("/gateway/overlay/lock/acquire", {
      name,
      ttl_secs: options.ttlSecs ?? 30,
    }) as { guard_id: string; token: string };
    const guardId = data.guard_id;
    return new LockGuard(guardId, BigInt(data.token), async () => {
      await this._delete(`/gateway/overlay/lock/${guardId}`);
    });
  }

  /**
   * One-shot leader election for `group`.
   * Returns the elected node's `"ip:port"` string.
   */
  async electLeader(group: string): Promise<string> {
    const data = await this._post("/gateway/overlay/elect", { group }) as { leader: string };
    return data.leader;
  }

  /**
   * Proposes `value` for `slot` requiring independent quorum from each group.
   *
   * Commits only when **all** specified groups individually reach their
   * `quorum` fraction. A single ballot round — no partial commitment possible.
   *
   * @param slot   - Consensus slot name (namespaced by the caller).
   * @param value  - Payload bytes to commit.
   * @param groups - Per-group quorum requirements.
   *
   * @example
   * ```ts
   * await agent.crossGroupPropose("pipeline/config", Buffer.from("v2"), [
   *   { group: "llm-workers", quorum: 0.5, veto: false },
   *   { group: "compliance",  quorum: 0.5, veto: true  },
   * ]);
   * ```
   */
  async crossGroupPropose(
    slot: string,
    value: Buffer | Uint8Array,
    groups: Array<{ group: string; quorum?: number; veto?: boolean }>,
  ): Promise<void> {
    await this._post("/gateway/consensus/cross_group_propose", {
      slot,
      value_b64: b64(value),
      groups: groups.map((g) => ({
        group:  g.group,
        quorum: g.quorum ?? 0.5,
        veto:   g.veto   ?? false,
      })),
    });
  }

  /**
   * Appends `value` to the named log stream.
   * Returns the HLC timestamp (use as cursor for `scanLog` or `subscribeLog`).
   */
  async append(stream: string, value: Buffer | Uint8Array = Buffer.alloc(0)): Promise<bigint> {
    const data = await this._post("/gateway/overlay/log/append", {
      stream,
      value_b64: b64(value),
    }) as { hlc: string };
    return BigInt(data.hlc);
  }

  /**
   * Range scan over a log stream. Returns `LogEntry[]` sorted by HLC.
   */
  async scanLog(
    stream: string,
    options: { fromHlc?: bigint; toHlc?: bigint } = {},
  ): Promise<LogEntry[]> {
    const params: Record<string, string> = { stream };
    if (options.fromHlc !== undefined) params.from_hlc = options.fromHlc.toString();
    if (options.toHlc !== undefined) params.to_hlc = options.toHlc.toString();
    const data = await this._get("/gateway/overlay/log/scan", params) as {
      entries: Array<{ hlc: string; value_b64: string }>;
    };
    return data.entries.map((e) => ({
      hlc: BigInt(e.hlc),
      value: fromb64(e.value_b64),
    }));
  }

  /** Tombstones all entries with `hlc < beforeHlc`. Gossips tombstones to peers. */
  async compactLog(stream: string, beforeHlc: bigint): Promise<void> {
    await this._post("/gateway/overlay/log/compact", {
      stream,
      before_hlc: beforeHlc.toString(),
    });
  }

  /**
   * Live SSE subscription. Yields new entries as they arrive, starting from `sinceHlc`.
   */
  async *subscribeLog(stream: string, options: { sinceHlc?: bigint } = {}): AsyncGenerator<LogEntry> {
    const params: Record<string, string> = { stream };
    if (options.sinceHlc !== undefined) params.since_hlc = options.sinceHlc.toString();
    const url = this._sseUrl("/gateway/overlay/log/subscribe", params);
    yield* sseStream<LogEntry>(url, (data) => {
      const raw = JSON.parse(data) as { hlc: string; value_b64: string };
      return { hlc: BigInt(raw.hlc), value: fromb64(raw.value_b64) };
    });
  }

  /**
   * Consumer-group subscription: at most one consumer per group processes an
   * entry at a time. The offset is persisted in the gossip KV.
   */
  async *subscribeLogGroup(stream: string, group: string): AsyncGenerator<LogEntry> {
    const url = this._sseUrl("/gateway/overlay/log/group/subscribe", { stream, group });
    yield* sseStream<LogEntry>(url, (data) => {
      const raw = JSON.parse(data) as { hlc: string; value_b64: string };
      return { hlc: BigInt(raw.hlc), value: fromb64(raw.value_b64) };
    });
  }

  /**
   * Sends `payload` to `target` and waits for an explicit application-level ACK.
   * Returns `"acknowledged"` or `"timeout"`.
   */
  async emitReliable(
    target: string,
    kind: string,
    payload: Buffer | Uint8Array = Buffer.alloc(0),
    options: { timeoutSecs?: number } = {},
  ): Promise<"acknowledged" | "timeout"> {
    const data = await this._post("/gateway/overlay/emit_reliable", {
      target,
      kind,
      payload_b64: b64(payload),
      timeout_secs: options.timeoutSecs ?? 5,
    }) as { status: "acknowledged" | "timeout" };
    return data.status;
  }

  // ── Cluster sharding ────────────────────────────────────────────────────

  /**
   * Returns the consistent-hash owner node-id for `key` among providers of `ns/name`.
   * Throws when no providers match the filter.
   */
  async shardFor(ns: string, name: string, key: string): Promise<string> {
    const resp = await fetch(
      `${this._base}gateway/shard/${encodeURIComponent(ns)}/${encodeURIComponent(name)}?key=${encodeURIComponent(key)}`,
    );
    if (resp.status === 404) throw new Error(`no providers for ${ns}/${name}`);
    if (!resp.ok) throw new Error(`shardFor failed: ${resp.status}`);
    const data = (await resp.json()) as { owner: string };
    return data.owner;
  }

  /**
   * Emits `kind` signal to the consistent-hash owner for `key` among providers of `ns/name`.
   * Returns the owner node-id string. Throws when no providers match the filter.
   */
  async emitSharded(
    kind: string,
    ns: string,
    name: string,
    key: string,
    payload: Buffer | Uint8Array = Buffer.alloc(0),
  ): Promise<string> {
    const data = await this._post("/gateway/shard/emit", {
      kind,
      ns,
      name,
      shard_key:   key,
      payload_b64: b64(payload),
    }) as { ok: boolean; owner?: string; error?: string };
    if (!data.ok) throw new Error(data.error ?? "no providers");
    return data.owner!;
  }
}
