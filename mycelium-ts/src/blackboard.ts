/**
 * mycelium/blackboard — TypeScript client for the Mycelium Blackboard.
 *
 * Wraps the `/gateway/bb/*` endpoints exposed by a Rust node running the
 * `mycelium-blackboard` companion crate with the `gateway` feature.
 *
 * The blackboard is shared working memory: agents `post` typed facts that any
 * agent can `read` (non-destructive, concurrent — Linda's `rd`), and a finite
 * fact is consumed by exactly one agent via `claim` (competitive, destructive —
 * Linda's `in`). Routing is by *content*: a claim names a predicate over fact
 * attributes, and which agent acts is decided by the fact, not by where it was put.
 *
 * @example
 * ```ts
 * import { Blackboard } from "mycelium-ts";
 * const bb = new Blackboard("127.0.0.1", 7946, "microgrid");
 * const claimed = await bb.claim({ kind: "surplus", feeder: "4" });
 * if (claimed) {
 *   const [id, attrs, payload] = claimed;
 *   await bb.ack(id);            // consumed once
 * }
 * ```
 */

export class BlackboardNotFoundError extends Error {
  constructor(id: number) {
    super(`unknown claim id ${id}`);
    this.name = "BlackboardNotFoundError";
  }
}

/** A fact: `[id, attributes, payload]`. */
export type Fact = [number, Record<string, string>, Uint8Array];

function toB64(data: Uint8Array): string {
  return Buffer.from(data).toString("base64");
}
function fromB64(b64: string): Uint8Array {
  return new Uint8Array(Buffer.from(b64, "base64"));
}
function decodeFact(f: { id: number; attributes: Record<string, string>; payload_b64: string }): Fact {
  return [f.id, f.attributes, fromB64(f.payload_b64)];
}

/** Async client for one board namespace via a node's HTTP gateway. */
export class Blackboard {
  private readonly baseUrl: string;
  constructor(
    host: string,
    port: number,
    private readonly ns: string = "board",
  ) {
    this.baseUrl = `http://${host}:${port}`;
  }

  /** Post a fact (Linda `out`) — non-destructive. Returns the fact id. */
  async post(attributes: Record<string, string>, payload: Uint8Array): Promise<number> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/post`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, attributes, payload_b64: toB64(payload) }),
    });
    if (!r.ok) throw new Error(`blackboard post failed: ${r.status}`);
    return ((await r.json()) as { id: number }).id;
  }

  /** Non-destructive read (Linda `rd`): all facts matching the predicate. */
  async read(eq: Record<string, string> = {}, present: string[] = []): Promise<Fact[]> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, eq, present }),
    });
    if (!r.ok) throw new Error(`blackboard read failed: ${r.status}`);
    const body = (await r.json()) as { facts: { id: number; attributes: Record<string, string>; payload_b64: string }[] };
    return body.facts.map(decodeFact);
  }

  /** Competitive destructive claim (Linda `in`): claim one matching fact, or `null`. */
  async claim(eq: Record<string, string> = {}, present: string[] = []): Promise<Fact | null> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/claim`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, eq, present }),
    });
    if (!r.ok) throw new Error(`blackboard claim failed: ${r.status}`);
    const body = (await r.json()) as { claimed: boolean; fact?: { id: number; attributes: Record<string, string>; payload_b64: string } };
    return body.claimed && body.fact ? decodeFact(body.fact) : null;
  }

  /** Terminal ack: the claimed fact was consumed. */
  async ack(id: number): Promise<void> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/ack`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, id }),
    });
    if (r.status === 404) throw new BlackboardNotFoundError(id);
    if (!r.ok) throw new Error(`blackboard ack failed: ${r.status}`);
  }

  /** Release a claim: the fact returns to claimable. */
  async release(id: number): Promise<void> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/release`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ns: this.ns, id }),
    });
    if (r.status === 404) throw new BlackboardNotFoundError(id);
    if (!r.ok) throw new Error(`blackboard release failed: ${r.status}`);
  }

  /** Live `[available, inflight]` counts for the board. */
  async depth(): Promise<[number, number]> {
    const r = await fetch(`${this.baseUrl}/gateway/bb/depth?ns=${encodeURIComponent(this.ns)}`);
    if (!r.ok) throw new Error(`blackboard depth failed: ${r.status}`);
    const body = (await r.json()) as { available: number; inflight: number };
    return [body.available, body.inflight];
  }
}
