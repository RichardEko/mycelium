/**
 * mycelium/wiki — TypeScript client for the Mycelium group wiki.
 *
 * Wraps the `/gateway/wiki/*` endpoints exposed by a Rust node running the
 * `mycelium-wiki` companion crate with the `gateway` feature.
 *
 * The wiki is the group's durable, curated knowledge canon — the long-term-memory
 * sibling of the blackboard's working memory. `read`/`query` are served directly
 * from the store (any node); `propose` enqueues an edit that the group's elected
 * curator applies (single writer of record). It composes with an external metrics
 * store (Postgres) and RAG by a shared id namespace — it is the *authoritative,
 * maintained-meaning* layer, not a similarity index.
 *
 * @example
 * ```ts
 * import { Wiki } from "mycelium-ts";
 * const wiki = new Wiki("127.0.0.1", 7946, "council");
 * const { proposal } = await wiki.propose({ page: "decisions/elm-street", heading: "Resolution 2026-14", body: "bike lane approved" });
 * const page = await wiki.read("decisions/elm-street");
 * const hits = await wiki.query({ topic: "transport" });
 * ```
 */

/** A section as read: stable opaque id, editable heading + body, and join-key/scope attributes. */
export interface Section {
  id: string;
  heading: string;
  body: string;
  attributes: Record<string, string>;
}

/** A page as read: the manifest joined with its live sections, in render order. */
export interface Page {
  path: string;
  attributes: Record<string, string>;
  sections: Section[];
}

/** A lightweight query hit — which section on which page matched, without the full-page join. */
export interface SectionRef {
  page: string;
  id: string;
  heading: string;
  attributes: Record<string, string>;
}

export interface ProposeArgs {
  page: string;
  /** Omit to mint a new section; provide an existing id to edit it. */
  section?: string;
  heading?: string;
  body: string;
  attributes?: Record<string, string>;
}

/** Async client for one group's wiki via a node's HTTP gateway. */
export class Wiki {
  private readonly baseUrl: string;
  constructor(
    host: string,
    port: number,
    private readonly group: string = "wiki",
  ) {
    this.baseUrl = `http://${host}:${port}`;
  }

  /** Read a page (manifest + live sections), or `null` if it has no manifest. Served directly from the store. */
  async read(page: string): Promise<Page | null> {
    const r = await fetch(`${this.baseUrl}/gateway/wiki/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group: this.group, page }),
    });
    if (!r.ok) throw new Error(`wiki read failed: ${r.status}`);
    return ((await r.json()) as { page: Page | null }).page;
  }

  /** Query sections by attribute (all-of equality — structured filter, not similarity search). */
  async query(equals: Record<string, string> = {}): Promise<SectionRef[]> {
    const r = await fetch(`${this.baseUrl}/gateway/wiki/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group: this.group, equals }),
    });
    if (!r.ok) throw new Error(`wiki query failed: ${r.status}`);
    return ((await r.json()) as { hits: SectionRef[] }).hits;
  }

  /** Propose an edit; the curator applies it. Returns the proposal key + the (minted or given) section id. */
  async propose(args: ProposeArgs): Promise<{ proposal: string; section: string }> {
    const r = await fetch(`${this.baseUrl}/gateway/wiki/propose`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group: this.group, ...args }),
    });
    if (!r.ok) throw new Error(`wiki propose failed: ${r.status}`);
    return (await r.json()) as { proposal: string; section: string };
  }
}
