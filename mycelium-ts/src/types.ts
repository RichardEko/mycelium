/** An admitted signal delivered to a local handler. */
export interface Signal {
  kind: string;
  sender: string;
  /** Raw payload bytes. */
  payload: Buffer;
  /** Random 64-bit nonce used for deduplication. */
  nonce: bigint;
}

/** A single entry in an ordered log stream. */
export interface LogEntry {
  /** HLC timestamp — use as a cursor for range scans. */
  hlc: bigint;
  value: Buffer;
}

/** An incoming RPC request. Call `rpc_respond` to complete the round-trip. */
export interface RpcRequest {
  kind: string;
  /** Hex-encoded 8-byte correlation nonce. */
  nonceHex: string;
  sender: string;
  payload: Buffer;
}

/** A mailbox event delivered to this node. */
export interface MailboxEvent {
  kind: string;
  sender: string;
  payload: Buffer;
}

/** Demand pressure for a capability. */
export interface DemandStatus {
  ns: string;
  name: string;
  providers: number;
  requirers: number;
  demandPressure: number;
}

/** Held while a distributed lock is acquired. Use as async context manager. */
export class LockGuard {
  /** Opaque guard ID used to release via HTTP. */
  readonly guardId: string;
  /** Monotonic fencing token (consensus ballot). */
  readonly token: bigint;

  private readonly _release: () => Promise<void>;

  constructor(guardId: string, token: bigint, release: () => Promise<void>) {
    this.guardId = guardId;
    this.token = token;
    this._release = release;
  }

  /** Releases the lock. */
  async release(): Promise<void> {
    return this._release();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    return this._release();
  }
}

/** Held while a capability advertisement is live. Drop to retract. */
export class CapabilityHandle {
  /** Opaque handle ID used to retract via HTTP. */
  readonly handleId: string;
  private readonly _drop: () => Promise<void>;
  private readonly _heartbeat?: () => Promise<void>;

  constructor(
    handleId: string,
    drop: () => Promise<void>,
    heartbeat?: () => Promise<void>,
  ) {
    this.handleId = handleId;
    this._drop = drop;
    this._heartbeat = heartbeat;
  }

  /** Retracts (tombstones) the capability. */
  async drop(): Promise<void> {
    return this._drop();
  }

  /**
   * Renews the lease on an advertisement made with `leaseSecs`. Must be
   * called within every `leaseSecs` window or the node retracts the advert.
   * Throws if the advertisement was made without a lease.
   */
  async heartbeat(): Promise<void> {
    if (!this._heartbeat) {
      throw new Error(
        "capability advertised without leaseSecs — no lease to renew",
      );
    }
    return this._heartbeat();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    return this._drop();
  }
}
