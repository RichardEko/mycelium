/**
 * A2A (Agent-to-Agent) protocol client for Mycelium.
 *
 * Lets TypeScript callers discover and invoke skills on A2A-protocol nodes
 * (requires the `a2a` cargo feature on the server side).
 *
 * @example
 * ```typescript
 * const client = new A2aClient("http://localhost:8300");
 * const card   = await client.fetchCard();
 * const reply  = await client.send("compute/gpu", "hello");
 * for await (const event of client.stream("compute/gpu", "hello")) {
 *   console.log(event.status.state);
 * }
 * ```
 */

// ── Wire types ─────────────────────────────────────────────────────────────

export interface AgentCard {
  name:         string;
  url:          string;
  version:      string;
  capabilities: A2aCapabilities;
  skills:       AgentSkill[];
}

export interface A2aCapabilities {
  streaming: boolean;
}

export interface AgentSkill {
  id:          string;
  name:        string;
  description: string;
}

export interface Task {
  id:        string;
  status:    TaskStatus;
  artifacts?: Artifact[];
}

export interface TaskStatus {
  state: "submitted" | "working" | "completed" | "failed" | "canceled";
}

export interface Artifact {
  parts: Part[];
}

export type Part = { type: "text"; text: string };

export interface TaskStatusUpdate {
  id:     string;
  status: TaskStatus;
  /** Present when state is "completed". */
  artifacts?: Artifact[];
  error?: string;
}

// ── Client ─────────────────────────────────────────────────────────────────

/**
 * HTTP client for A2A-protocol Mycelium nodes.
 *
 * All methods use the standard `fetch` API — no runtime-specific dependencies.
 */
export class A2aClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;

  constructor(agentCardUrl: string, { timeoutMs = 30_000 }: { timeoutMs?: number } = {}) {
    this.baseUrl   = agentCardUrl.replace(/\/$/, "");
    this.timeoutMs = timeoutMs;
  }

  // ── Discovery ───────────────────────────────────────────────────────────

  /** Fetch the AgentCard from `/.well-known/agent.json`. */
  async fetchCard(): Promise<AgentCard> {
    const resp = await fetch(`${this.baseUrl}/.well-known/agent.json`, {
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!resp.ok) throw new Error(`AgentCard fetch failed: ${resp.status}`);
    return resp.json() as Promise<AgentCard>;
  }

  // ── Synchronous dispatch ────────────────────────────────────────────────

  /**
   * Send a `tasks/send` request and return the reply text.
   *
   * @param skillId  Skill ID in `"ns/name"` format, e.g. `"compute/gpu"`.
   * @param message  Plain-text input to the skill.
   * @returns First text part of the completed task's first artifact.
   * @throws {Error} On JSON-RPC error or HTTP failure.
   */
  async send(skillId: string, message: string): Promise<string> {
    const taskId  = crypto.randomUUID();
    const payload = {
      jsonrpc: "2.0",
      id:      1,
      method:  "tasks/send",
      params:  {
        id:       taskId,
        skillId,
        message:  { role: "user", parts: [{ type: "text", text: message }] },
      },
    };

    const resp = await fetch(`${this.baseUrl}/a2a`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
      signal:  AbortSignal.timeout(this.timeoutMs + 5_000),
    });
    if (!resp.ok) throw new Error(`/a2a responded ${resp.status}`);

    const body = (await resp.json()) as { result?: Task; error?: { code: number; message: string } };
    if (body.error) {
      throw new Error(`A2A error ${body.error.code}: ${body.error.message}`);
    }
    return extractText(body.result ?? { id: taskId, status: { state: "failed" } });
  }

  // ── SSE streaming ────────────────────────────────────────────────────────

  /**
   * Send a `tasks/sendSubscribe` request and yield status events until the
   * task reaches a terminal state (`completed`, `failed`, or `canceled`).
   *
   * @param skillId  Skill ID in `"ns/name"` format.
   * @param message  Plain-text input.
   */
  async *stream(skillId: string, message: string): AsyncGenerator<TaskStatusUpdate> {
    const taskId  = crypto.randomUUID();
    const payload = {
      jsonrpc: "2.0",
      id:      1,
      method:  "tasks/sendSubscribe",
      params:  {
        id:      taskId,
        skillId,
        message: { role: "user", parts: [{ type: "text", text: message }] },
      },
    };

    const resp = await fetch(`${this.baseUrl}/a2a`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(payload),
      signal:  AbortSignal.timeout(this.timeoutMs + 5_000),
    });
    if (!resp.ok) throw new Error(`/a2a SSE responded ${resp.status}`);
    if (!resp.body) throw new Error("/a2a SSE response has no body");

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let   buffer  = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          if (line.startsWith("data:")) {
            const data = line.slice(5).trim();
            if (!data) continue;
            try {
              const event = JSON.parse(data) as TaskStatusUpdate;
              yield event;
              const { state } = event.status;
              if (state === "completed" || state === "failed" || state === "canceled") return;
            } catch {
              // ignore malformed SSE data
            }
          }
        }
      }
    } finally {
      reader.cancel().catch(() => undefined);
    }
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────

function extractText(task: Task): string {
  const artifacts = task.artifacts ?? [];
  if (!artifacts.length) return "";
  const parts = artifacts[0].parts ?? [];
  if (!parts.length) return "";
  const part = parts[0];
  return part.type === "text" ? part.text : "";
}
