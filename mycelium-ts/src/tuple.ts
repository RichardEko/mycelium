/**
 * mycelium/tuple — TypeScript client for the Mycelium TupleSpace.
 *
 * Wraps the `/gateway/tuple/*` endpoints exposed by a Rust node running the
 * `mycelium-tuple-space` companion crate with the `gateway` feature.
 *
 * The tuple space is the pull-based work distribution pattern: workers
 * `take()` only when ready, so load balance emerges from worker readiness —
 * no coordinator predicts anything.
 *
 * Note — lanes, not Linda matching: unlike classic Linda's associative
 * template retrieval, this space is lane-addressed. Every op names a *stage*
 * (a per-stage FIFO lane); payloads are opaque and never matched. An item's
 * pipeline position is the lane it sits in, and `complete()` moves it
 * atomically to the next lane. A worker's only "filter" is choosing which
 * lane to `take()` from — use `depth()` to pick the deepest.
 *
 * @example
 * ```ts
 * import { TupleSpace } from "mycelium-ts";
 *
 * const ts = new TupleSpace("127.0.0.1", 7946, "news-pipeline");
 * for (;;) {
 *   const [itemId, payload] = await ts.take("stage-a", 60);
 *   const result = await runLlm(payload);          // seconds of compute
 *   await ts.complete(itemId, "stage-b", result);  // atomic: no crash window
 * }
 * ```
 */

/** The primary is saturated (HTTP 503). Back off and retry. */
export class TupleBackpressureError extends Error {
  constructor(public retryAfterMs: number) {
    super(`tuple-space backpressure; retry after ${retryAfterMs} ms`);
    this.name = "TupleBackpressureError";
  }
}

/** Unknown item id — already acked, expired back to the queue, or never existed. */
export class TupleNotFoundError extends Error {
  constructor(itemId: number) {
    super(`unknown item id ${itemId}`);
    this.name = "TupleNotFoundError";
  }
}

/** Depth snapshot for one stage. */
export interface StageDepth {
  depth: number;
  waiters: number;
  inflight: number;
}

function toB64(data: Uint8Array): string {
  return Buffer.from(data).toString("base64");
}

function fromB64(b64: string): Uint8Array {
  return new Uint8Array(Buffer.from(b64, "base64"));
}

/** Async client for one tuple space namespace via a node's HTTP gateway. */
export class TupleSpace {
  private baseUrl: string;

  constructor(
    host: string,
    port: number,
    private ns: string = "pipeline",
  ) {
    this.baseUrl = `http://${host}:${port}`;
  }

  /**
   * Write an item to `stage`. Returns the item id.
   *
   * `backpressure: "raise"` (default) throws {@link TupleBackpressureError}
   * immediately when the primary is saturated; `"block"` retries with
   * exponential backoff until `backpressureTimeoutSecs`.
   */
  async put(
    stage: string,
    payload: Uint8Array,
    opts?: {
      backpressure?: "raise" | "block";
      backpressureTimeoutSecs?: number;
    },
  ): Promise<number> {
    const deadline =
      Date.now() + (opts?.backpressureTimeoutSecs ?? 30) * 1000;
    let delay = 100;
    for (;;) {
      try {
        return await this.putOnce(stage, payload);
      } catch (e) {
        if (!(e instanceof TupleBackpressureError)) throw e;
        if (opts?.backpressure !== "block") throw e;
        if (Date.now() + delay >= deadline) throw e;
        await new Promise((r) => setTimeout(r, delay));
        delay = Math.min(delay * 2, 5000);
      }
    }
  }

  private async putOnce(stage: string, payload: Uint8Array): Promise<number> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/put`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ns: this.ns,
        stage,
        payload_b64: toB64(payload),
      }),
    });
    if (r.status === 503) {
      const retryAfter = Number(r.headers.get("Retry-After") ?? "1");
      throw new TupleBackpressureError(retryAfter * 1000);
    }
    if (!r.ok) throw new Error(`tuple put failed: ${r.status}`);
    const body = (await r.json()) as { id: number };
    return body.id;
  }

  /**
   * Blocking claim: resolves with `[itemId, payload]` or throws on timeout.
   * The HTTP request blocks server-side for up to `timeoutSecs`.
   */
  async take(
    stage: string,
    timeoutSecs: number = 30,
  ): Promise<[number, Uint8Array]> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/take`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, stage, timeout_secs: timeoutSecs }),
      signal: AbortSignal.timeout((timeoutSecs + 5) * 1000),
    });
    if (r.status === 408) {
      throw new Error(`no item on stage "${stage}" within ${timeoutSecs}s`);
    }
    if (!r.ok) throw new Error(`tuple take failed: ${r.status}`);
    const body = (await r.json()) as { id: number; payload_b64: string };
    return [body.id, fromB64(body.payload_b64)];
  }

  // ── Keyed-exact-match rendezvous (M13 — fan-in joins) ──────────────────────

  /**
   * Put `payload` on `stage` under correlation `key` (M13). Claimed only by a
   * matching {@link takeByKey} — the two-stream rendezvous ("an invoice AND its
   * matching purchase order"). Returns the item id.
   */
  async putKeyed(stage: string, key: string, payload: Uint8Array): Promise<number> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/put`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, stage, key, payload_b64: toB64(payload) }),
    });
    if (r.status === 503) {
      const retryAfter = Number(r.headers.get("Retry-After") ?? "1");
      throw new TupleBackpressureError(retryAfter * 1000);
    }
    if (!r.ok) throw new Error(`tuple putKeyed failed: ${r.status}`);
    const body = (await r.json()) as { id: number };
    return body.id;
  }

  /**
   * Blocking keyed claim (M13): claims the item on `stage` whose correlation key
   * is `key`, or parks until it arrives. Resolves with `[itemId, payload]` or
   * throws on timeout.
   */
  async takeByKey(
    stage: string,
    key: string,
    timeoutSecs: number = 30,
  ): Promise<[number, Uint8Array]> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/take_by_key`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, stage, key, timeout_secs: timeoutSecs }),
      signal: AbortSignal.timeout((timeoutSecs + 5) * 1000),
    });
    if (r.status === 408) {
      throw new Error(`no item keyed "${key}" on stage "${stage}" within ${timeoutSecs}s`);
    }
    if (!r.ok) throw new Error(`tuple takeByKey failed: ${r.status}`);
    const body = (await r.json()) as { id: number; payload_b64: string };
    return [body.id, fromB64(body.payload_b64)];
  }

  /**
   * Atomic pipeline advance: acks `itemId` AND puts `nextStage` in one WAL
   * record — no crash window between stages. PREFERRED over separate
   * put + ack for every mid-pipeline transition. Returns the new item id.
   */
  async complete(
    itemId: number,
    nextStage: string,
    payload: Uint8Array,
  ): Promise<number> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ns: this.ns,
        id: itemId,
        next_stage: nextStage,
        next_payload_b64: toB64(payload),
      }),
    });
    if (r.status === 404) throw new TupleNotFoundError(itemId);
    if (!r.ok) throw new Error(`tuple complete failed: ${r.status}`);
    const body = (await r.json()) as { next_id: number };
    return body.next_id;
  }

  /** Terminal ack: last stage of a pipeline or explicit abandonment. */
  async ack(itemId: number): Promise<void> {
    const r = await fetch(`${this.baseUrl}/gateway/tuple/ack`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, id: itemId }),
    });
    if (r.status === 404) throw new TupleNotFoundError(itemId);
    if (!r.ok) throw new Error(`tuple ack failed: ${r.status}`);
  }

  /** Returns `{stage: {depth, waiters, inflight}}` for one or all stages. */
  async depth(stage?: string): Promise<Record<string, StageDepth>> {
    const params = new URLSearchParams({ ns: this.ns });
    if (stage !== undefined) params.set("stage", stage);
    const r = await fetch(
      `${this.baseUrl}/gateway/tuple/depth?${params.toString()}`,
    );
    if (!r.ok) throw new Error(`tuple depth failed: ${r.status}`);
    const body = (await r.json()) as {
      stages: Array<{
        stage: string;
        depth: number;
        waiters: number;
        inflight: number;
      }>;
    };
    const out: Record<string, StageDepth> = {};
    for (const s of body.stages) {
      out[s.stage] = {
        depth: s.depth,
        waiters: s.waiters,
        inflight: s.inflight,
      };
    }
    return out;
  }
}
