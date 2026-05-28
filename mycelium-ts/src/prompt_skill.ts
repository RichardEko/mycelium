/**
 * mycelium/prompt_skill — TypeScript client for LLM Prompt Skills.
 *
 * Wraps the `/gateway/prompts/*` and `/gateway/llm/*` endpoints exposed by a
 * Rust Mycelium node compiled with `--features llm`.
 *
 * @example
 * ```ts
 * import { PromptSkillClient, PromptTemplate } from "mycelium-ts";
 *
 * const client = new PromptSkillClient("127.0.0.1", 7946);
 *
 * await client.updatePrompt("ai", "chat", {
 *   system: "You are a helpful assistant.",
 *   userTemplate: "{{input}}",
 *   maxTokens: 512,
 *   temperature: 0.7,
 *   metadata: {},
 * });
 *
 * const result = await client.call("ai", "chat", "Hello!");
 * console.log(result.output);
 *
 * const entries = await client.list();
 * for (const e of entries) console.log(e.ns, e.name);
 *
 * await client.close();
 * ```
 */

/** Mirror of the Rust `PromptTemplate` struct stored in cluster KV. */
export interface PromptTemplate {
  /** System prompt. May contain `{{variable}}` placeholders. */
  system: string;
  /** User message template. Must contain at least `{{input}}`. */
  userTemplate: string;
  /** Maximum tokens in the LLM response. */
  maxTokens: number;
  /** Sampling temperature. `0.0` = deterministic. */
  temperature: number;
  /** Arbitrary metadata (tags, version notes, model hints). */
  metadata: Record<string, unknown>;
}

/** Result returned by {@link PromptSkillClient.call}. */
export interface CallResult {
  /** LLM-generated output text. */
  output: string;
  /** NodeId of the provider that served the request. */
  provider: string;
}

/** Summary entry returned by {@link PromptSkillClient.list}. */
export interface PromptListEntry {
  ns: string;
  name: string;
  maxTokens: number;
  temperature: number;
  metadata: Record<string, unknown>;
}

/** Wire shape sent to/from the Rust gateway (snake_case). */
interface TemplateWire {
  system: string;
  user_template: string;
  max_tokens: number;
  temperature: number;
  metadata: Record<string, unknown>;
}

interface ListEntryWire {
  ns: string;
  name: string;
  max_tokens: number;
  temperature: number;
  metadata: Record<string, unknown>;
}

function templateToWire(t: PromptTemplate): TemplateWire {
  return {
    system: t.system,
    user_template: t.userTemplate,
    max_tokens: t.maxTokens,
    temperature: t.temperature,
    metadata: t.metadata,
  };
}

function templateFromWire(w: TemplateWire): PromptTemplate {
  return {
    system: w.system,
    userTemplate: w.user_template,
    maxTokens: w.max_tokens,
    temperature: w.temperature,
    metadata: w.metadata,
  };
}

/**
 * HTTP client for the Mycelium Prompt Skills gateway.
 *
 * Talks to `/gateway/prompts/*` and `/gateway/llm/*` on the Rust node.
 * Requires the node to be compiled with `--features llm`.
 */
export class PromptSkillClient {
  private readonly baseUrl: string;
  private readonly defaultTimeoutMs: number;

  constructor(
    host: string,
    port = 8080,
    timeoutMs = 30_000,
  ) {
    this.baseUrl = `http://${host}:${port}`;
    this.defaultTimeoutMs = timeoutMs;
  }

  // ── Template management ────────────────────────────────────────────────────

  /**
   * List all prompt templates visible in the local KV snapshot.
   *
   * Returns a summary list; use {@link get} to fetch the full template.
   */
  async list(): Promise<PromptListEntry[]> {
    const resp = await this.fetch("/gateway/prompts");
    const raw: ListEntryWire[] = await resp.json();
    return (Array.isArray(raw) ? raw : []).map((w) => ({
      ns:          w.ns,
      name:        w.name,
      maxTokens:   w.max_tokens,
      temperature: w.temperature,
      metadata:    w.metadata,
    }));
  }

  /**
   * Retrieve a specific prompt template from the local KV snapshot.
   *
   * Returns `null` if the key does not exist.
   */
  async get(ns: string, name: string): Promise<PromptTemplate | null> {
    const resp = await this.fetch(`/gateway/prompts/${ns}/${name}`, {
      ignoreNotFound: true,
    });
    if (resp.status === 404) return null;
    const w: TemplateWire = await resp.json();
    return templateFromWire(w);
  }

  /**
   * Write (or overwrite) a prompt template in the cluster KV.
   *
   * The change propagates to all nodes via gossip. Serving nodes read the
   * template fresh from KV on every invocation, so the update takes effect
   * immediately without restarting any skill handler.
   */
  async updatePrompt(ns: string, name: string, template: PromptTemplate): Promise<void> {
    await this.fetch(`/gateway/prompts/${ns}/${name}`, {
      method: "PUT",
      body: JSON.stringify(templateToWire(template)),
      headers: { "Content-Type": "application/json" },
    });
  }

  /**
   * Tombstone a prompt template in the cluster KV.
   *
   * The skill becomes unreachable once all serving nodes' capability entries
   * expire (within 30 s).
   */
  async deletePrompt(ns: string, name: string): Promise<void> {
    await this.fetch(`/gateway/prompts/${ns}/${name}`, { method: "DELETE" });
  }

  // ── Skill invocation ───────────────────────────────────────────────────────

  /**
   * Invoke a prompt skill via the gateway.
   *
   * Resolves a provider for capability `(ns, name)`, sends an `llm.invoke` RPC,
   * and returns `{ output, provider }`.
   *
   * @param ns         Capability namespace (e.g. `"ai"`).
   * @param name       Capability name (e.g. `"chat"`).
   * @param input      The `{{input}}` value rendered into the template.
   * @param context    Optional extra `{{variable}}` substitutions.
   * @param timeoutMs  RPC timeout in milliseconds.
   */
  async call(
    ns: string,
    name: string,
    input: string,
    context: Record<string, string> = {},
    timeoutMs: number = this.defaultTimeoutMs,
  ): Promise<CallResult> {
    const resp = await this.fetch("/gateway/llm/call", {
      method: "POST",
      body: JSON.stringify({ ns, name, input, context, timeout_ms: timeoutMs }),
      headers: { "Content-Type": "application/json" },
    });
    const data = await resp.json();
    if (data.error) {
      throw new Error(`llm call failed: ${data.error}: ${data.detail ?? ""}`);
    }
    return { output: String(data.output ?? ""), provider: String(data.provider ?? "") };
  }

  /** Close the client (no-op — fetch is stateless; kept for API symmetry). */
  close(): void { /* stateless fetch — nothing to close */ }

  // ── Internal helpers ───────────────────────────────────────────────────────

  private async fetch(
    path: string,
    opts: RequestInit & { ignoreNotFound?: boolean } = {},
  ): Promise<Response> {
    const { ignoreNotFound, ...fetchOpts } = opts;
    const resp = await globalThis.fetch(`${this.baseUrl}${path}`, fetchOpts);
    if (!resp.ok && !(ignoreNotFound && resp.status === 404)) {
      const text = await resp.text().catch(() => "");
      throw new Error(`Mycelium gateway error ${resp.status}: ${text}`);
    }
    return resp;
  }
}
