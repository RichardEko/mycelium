import { createParser, type ParseEvent } from "eventsource-parser";

interface Waiter {
  resolve: () => void;
  reject: (e: Error) => void;
}

/**
 * Reads an SSE stream from `url` (GET request) and yields parsed objects.
 * Terminates when the server closes the connection or the caller breaks out.
 */
export async function* sseStream<T>(
  url: string,
  parse: (data: string) => T,
): AsyncGenerator<T> {
  const resp = await fetch(url);
  if (!resp.ok || !resp.body) {
    throw new Error(`SSE request failed: ${resp.status} ${resp.statusText}`);
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();

  const waiterBox: { current: Waiter | null } = { current: null };
  const pending: T[] = [];
  let streamDone = false;
  let streamError: Error | null = null;

  function wake(): void {
    const w = waiterBox.current;
    if (w) { waiterBox.current = null; w.resolve(); }
  }

  const parser = createParser((event: ParseEvent) => {
    if (event.type !== "event") return;
    if (event.data === "[DONE]") return;
    try {
      pending.push(parse(event.data));
      wake();
    } catch {
      // skip malformed frames
    }
  });

  void (async () => {
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        parser.feed(decoder.decode(chunk.value, { stream: true }));
      }
    } catch (e) {
      streamError = e instanceof Error ? e : new Error(String(e));
      const w = waiterBox.current;
      if (w) { waiterBox.current = null; w.reject(streamError); }
      return;
    }
    streamDone = true;
    wake();
  })();

  while (true) {
    while (pending.length > 0) {
      yield pending.shift()!;
    }
    if (streamDone) break;
    if (streamError) throw streamError;
    await new Promise<void>((res, rej) => {
      waiterBox.current = { resolve: res, reject: rej };
    });
  }
}
