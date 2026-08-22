/**
 * @fileoverview SequencedChannel — ordered delivery, gap detection, and
 * generations for a one-way stream of values.
 *
 * A channel is a pure message stream, not a value handler: `canHandle()`
 * always returns `false`, so a `SequencedChannel` never claims values during
 * ordinary argument/return serialization. Everything flows over the handler
 * messaging channel (`connect()` / `onMessage()` / `disconnect()`), riding
 * the existing `{type: 'handler', wireType, payload}` envelope — no new wire
 * message type.
 *
 * The library owns two things: strict in-order delivery, and gap detection
 * with optional self-healing via replay. The application owns domain
 * recovery — deciding what a resync sends, and calling `newGeneration()`
 * when it does.
 *
 * @example
 * ```ts
 * import {SequencedChannel} from '@eamodio/supertalk-core/handlers/channel.js';
 *
 * // Sender
 * const rows = new SequencedChannel<RowSplice>('rows', {replay: 32});
 * expose(service, endpoint, {handlers: [rows]});
 * rows.send(splice); // stamped {generation, seq}
 * rows.newGeneration(); // epoch bump: resets seq, clears replay, invalidates in-flight
 *
 * // Receiver
 * const rows = new SequencedChannel<RowSplice>('rows');
 * const connection = new Connection(endpoint, {handlers: [rows]});
 * rows.subscribe((splice, meta) => ledger.apply(splice)); // in-order only
 * rows.onGap((gap) => void service.resyncRows()); // domain recovery is the app's
 * ```
 */

import type {Handler, HandlerConnectionContext} from '../lib/types.js';

/**
 * Options for {@link SequencedChannel}.
 */
export interface SequencedChannelOptions {
  /** Keep the last N sent messages so a receiver gap can be repaired automatically. */
  replay?: number;
}

/** Metadata delivered alongside each value. */
export interface ChannelMeta {
  generation: number;
  seq: number;
}

/**
 * Reported exactly once per gap, only when the channel cannot self-heal —
 * either no replay is configured, or the sender no longer has the missing
 * messages buffered. The channel stays gapped (dropping same-generation
 * messages) until `newGeneration()` is called.
 */
export interface ChannelGap {
  generation: number;
  /** The seq the receiver expected. */
  expected: number;
  /** The seq that first exposed the gap. */
  received: number;
}

// Wire payload shapes. Keys are kept short — they ride on every message.
// Discriminated by property presence rather than a `type` tag, since these
// are internal to the channel and never observed outside it.

interface DataPayload {
  g: number;
  s: number;
  v: unknown;
}

interface EpochPayload {
  g: number;
  e: 1;
}

interface ReplayRequestPayload {
  /** The generation the request is about. */
  g: number;
  /** The seq the receiver expects next. */
  r: number;
}

interface ReplayMissPayload {
  /** The generation the miss is about. */
  g: number;
  /** The seq that was requested and could not be replayed. */
  m: number;
}

function has<K extends string>(
  payload: unknown,
  key: K,
): payload is Record<K, unknown> {
  return typeof payload === 'object' && payload !== null && key in payload;
}

function isDataPayload(payload: unknown): payload is DataPayload {
  return has(payload, 's');
}

function isEpochPayload(payload: unknown): payload is EpochPayload {
  return has(payload, 'e');
}

function isReplayRequestPayload(
  payload: unknown,
): payload is ReplayRequestPayload {
  return has(payload, 'r');
}

function isReplayMissPayload(payload: unknown): payload is ReplayMissPayload {
  return has(payload, 'm');
}

interface ReplayEntry<T> {
  seq: number;
  value: T;
}

/**
 * Ordered, gap-detecting one-way stream, with sender-side generations
 * (epochs) and optional receiver-side self-healing via replay.
 *
 * Create one instance per channel per side, and register it in `handlers`
 * on both `expose()`/`Connection` and `wrap()`/`Connection`. Instances are
 * symmetric: each tracks its own outbound sequence and its own inbound
 * expectation, so a single pair of instances can carry traffic both ways.
 * N channels means N handler entries.
 *
 * Guarantee: every value is delivered in order, or an `onGap` event fires —
 * never both, and never neither. The library owns ordering and gap
 * detection; the application owns domain recovery (what a resync sends, and
 * calling `newGeneration()` when it does).
 *
 * A generation can be adopted from a data message rather than the epoch
 * announcement — e.g. the announcement itself, or the new generation's
 * early messages, were dropped while the receiver was unreachable (a hidden
 * VS Code webview silently drops `postMessage` to a retained-but-invisible
 * view). Adopting at `seq === 0` is the ordinary case: nothing was missed,
 * so the value delivers immediately. Adopting at `seq > 0` means the sender
 * is already past the start of the new generation, so the receiver no
 * longer assumes it is safe to start mid-stream: it adopts the generation
 * without delivering, opens a gap, and requests a replay from seq 0 — from
 * there it behaves exactly like an in-generation gap, healing invisibly or
 * reporting `onGap`.
 */
export class SequencedChannel<T> implements Handler<never, never> {
  readonly wireType: string;

  readonly #replayLimit: number;

  #ctx: HandlerConnectionContext | undefined;

  // Sender (outbound) state.
  #generation = 0;
  #seq = 0;
  #replayBuffer: Array<ReplayEntry<T>> = [];

  // Receiver (inbound) state. `#inboundGeneration` is `undefined` until the
  // first data or epoch message arrives.
  #inboundGeneration: number | undefined;
  #expected = 0;
  #gapped = false;
  // The seq that first exposed the current gap — reported as `received`.
  #gapReceivedSeq = 0;
  // The highest seq seen while gapped. A replay re-request only fires when
  // this advances, so re-replayed duplicates don't retrigger it, but each
  // new live message re-arms recovery if the previous request or its
  // response was lost.
  #gapHighestSeq = 0;
  // Whether onGap has already fired for the current gap — reset alongside
  // #gapped so a gap reports exactly once, even if recovery is re-requested
  // multiple times before it's resolved.
  #gapReported = false;

  #listeners = new Set<(value: T, meta: ChannelMeta) => void>();
  #gapListeners = new Set<(gap: ChannelGap) => void>();

  constructor(name: string, options: SequencedChannelOptions = {}) {
    this.wireType = `st:ch:${name}`;
    this.#replayLimit = options.replay ?? 0;
  }

  /** The outbound epoch — bumped by {@link newGeneration}. */
  get generation(): number {
    return this.#generation;
  }

  /** Whether this instance is currently attached to a connection. */
  get connected(): boolean {
    return this.#ctx !== undefined;
  }

  /** Inbound: `true` while waiting on recovery (a replay, or a new generation). */
  get gapped(): boolean {
    return this.#gapped;
  }

  /**
   * Send a value. Stamped with the current generation and the next seq.
   *
   * If not connected, this does nothing and does NOT consume a sequence
   * number — the next connect starts a fresh generation, so there is no
   * seq gap for a peer to detect.
   */
  send(value: T): void {
    if (this.#ctx === undefined) return;

    const seq = this.#seq++;
    if (this.#replayLimit > 0) {
      this.#replayBuffer.push({seq, value});
      while (this.#replayBuffer.length > this.#replayLimit) {
        this.#replayBuffer.shift();
      }
    }
    this.#ctx.sendMessage({g: this.#generation, s: seq, v: value});
  }

  /**
   * Bump the outbound epoch: resets the outbound seq to 0, clears the
   * replay buffer, and — if connected — announces the new generation so
   * the peer invalidates any in-flight messages from the old one.
   */
  newGeneration(): number {
    this.#generation++;
    this.#seq = 0;
    this.#replayBuffer.length = 0;
    if (this.#ctx !== undefined) {
      this.#ctx.sendMessage({g: this.#generation, e: 1});
    }
    return this.#generation;
  }

  /** Subscribe to in-order delivery. Returns an unsubscribe function. */
  subscribe(listener: (value: T, meta: ChannelMeta) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Subscribe to gap events. Returns an unsubscribe function. */
  onGap(listener: (gap: ChannelGap) => void): () => void {
    this.#gapListeners.add(listener);
    return () => this.#gapListeners.delete(listener);
  }

  // A channel never claims values during ordinary serialization — it is a
  // pure message stream, wired up entirely through connect/onMessage.
  canHandle(_value: unknown): _value is never {
    return false;
  }

  toWire(): never {
    throw new Error(
      `SequencedChannel (${this.wireType}) cannot serialize values directly — ` +
        'canHandle() always returns false, so toWire() should never be reached. ' +
        'Register it in `handlers` to carry messages, not to encode a value type.',
    );
  }

  connect(ctx: HandlerConnectionContext): void {
    this.#ctx = ctx;
  }

  /**
   * Reconnect lifecycle: clears the context, resets ALL inbound state (no
   * current generation, expected back to 0, gap flags cleared), and bumps
   * the outbound generation/seq/replay buffer WITHOUT announcing — since
   * there is no connection to announce over. The result is that the first
   * message of the next session carries a fresh epoch, so the peer resyncs
   * from scratch instead of reporting a spurious gap.
   */
  disconnect(): void {
    this.#ctx = undefined;

    this.#inboundGeneration = undefined;
    this.#expected = 0;
    this.#gapped = false;
    this.#gapReported = false;
    this.#gapReceivedSeq = 0;
    this.#gapHighestSeq = 0;

    this.#generation++;
    this.#seq = 0;
    this.#replayBuffer.length = 0;
  }

  onMessage(payload: unknown): void {
    // Data is every live message; the other three are rare control
    // messages, and the shapes are mutually exclusive, so checking data
    // first keeps the hot path a single guard.
    if (isDataPayload(payload)) {
      this.#handleData(payload.g, payload.s, payload.v as T);
    } else if (isEpochPayload(payload)) {
      this.#adoptGeneration(payload.g);
    } else if (isReplayRequestPayload(payload)) {
      this.#handleReplayRequest(payload.g, payload.r);
    } else if (isReplayMissPayload(payload)) {
      this.#handleReplayMiss(payload.g, payload.m);
    }
  }

  #adoptGeneration(g: number): void {
    // Generations are monotonic. Ignore a duplicate or reordered epoch
    // announcement for a generation already adopted — re-adopting would reset
    // #expected and open a spurious gap (re-delivering already-delivered seqs
    // via replay).
    if (this.#inboundGeneration !== undefined && g <= this.#inboundGeneration)
      return;

    this.#inboundGeneration = g;
    this.#expected = 0;
    this.#gapped = false;
    this.#gapReported = false;
    this.#gapReceivedSeq = 0;
    this.#gapHighestSeq = 0;
  }

  #handleData(g: number, s: number, v: T): void {
    if (this.#inboundGeneration === undefined || g > this.#inboundGeneration) {
      this.#inboundGeneration = g;
      if (s === 0) {
        // Fresh generation, nothing missed: adopt and deliver.
        this.#gapped = false;
        this.#gapReported = false;
        this.#gapHighestSeq = 0;
        this.#deliver(v, {generation: g, seq: s});
        this.#expected = 1;
      } else {
        // Adopted via a data message mid-stream: the epoch announcement and
        // seq 0..s-1 of the new generation may have been silently dropped
        // while this receiver was unreachable. Do not deliver from the
        // middle of the stream — enter the gapped state and request a
        // replay from seq 0, exactly like an in-generation gap.
        this.#expected = 0;
        this.#gapped = true;
        this.#gapReported = false;
        this.#gapReceivedSeq = s;
        this.#gapHighestSeq = s;
        this.#requestReplay();
      }
      return;
    }

    if (g < this.#inboundGeneration) return; // stale in-flight, drop

    // g === current generation.
    if (s === this.#expected) {
      // Delivering exactly the expected seq always clears a gap — this is
      // also how a replay resumes a gapped channel.
      this.#gapped = false;
      this.#gapReported = false;
      this.#gapHighestSeq = 0;
      this.#deliver(v, {generation: g, seq: s});
      this.#expected = s + 1;
      return;
    }

    if (s < this.#expected) return; // duplicate, drop

    // s > expected: a gap, either newly detected or still open.
    if (!this.#gapped) {
      // Gap. Request a replay; emit nothing yet — onGap only fires if the
      // sender can't repair it (see #handleReplayMiss).
      this.#gapped = true;
      this.#gapReported = false;
      this.#gapReceivedSeq = s;
      this.#gapHighestSeq = s;
      this.#requestReplay();
    } else if (s > this.#gapHighestSeq) {
      // A live message arrived past the seq we last requested against — the
      // previous request or its replay response may have been lost.
      // Re-request; the receiver's own duplicate/sequence rules make this
      // idempotent against replayed duplicates that don't advance the seq.
      // Once a miss has answered this gap, though, re-requesting is
      // pointless: the buffer only advances, so the same request must
      // always miss again.
      this.#gapHighestSeq = s;
      if (!this.#gapReported) {
        this.#requestReplay();
      }
    }
    // else still gapped and s <= gapHighestSeq: an already-requested-for
    // duplicate or replay artifact, drop without re-requesting.
  }

  #requestReplay(): void {
    if (this.#ctx !== undefined && this.#inboundGeneration !== undefined) {
      this.#ctx.sendMessage({g: this.#inboundGeneration, r: this.#expected});
    }
  }

  #handleReplayRequest(g: number, requested: number): void {
    if (g !== this.#generation) {
      // A request issued against a generation this sender has moved past —
      // nothing can be replayed for it. Answer with a miss tagged with THAT
      // generation so the receiver can correlate (or ignore, if it too has
      // moved on).
      this.#ctx?.sendMessage({g, m: requested});
      return;
    }
    const oldest = this.#replayBuffer[0];
    if (oldest === undefined || oldest.seq > requested) {
      this.#ctx?.sendMessage({g, m: requested});
      return;
    }
    // The buffer is contiguous (pushed in seq order, evicted from the front),
    // so the first entry to replay is at a computable offset.
    for (let i = requested - oldest.seq; i < this.#replayBuffer.length; i++) {
      const entry = this.#replayBuffer[i];
      if (entry === undefined) continue;
      this.#ctx?.sendMessage({
        g: this.#generation,
        s: entry.seq,
        v: entry.value,
      });
    }
  }

  #handleReplayMiss(g: number, missed: number): void {
    // Only report a miss that answers the currently open gap: same
    // generation, same expected seq, and not already reported — a stale or
    // duplicate miss (an earlier gap, an old generation, or a re-request
    // that was already answered) must not fire onGap again.
    if (!this.#gapped || this.#gapReported) return;
    if (g !== this.#inboundGeneration || missed !== this.#expected) return;
    this.#gapReported = true;

    const gap: ChannelGap = {
      generation: this.#inboundGeneration,
      expected: this.#expected,
      received: this.#gapReceivedSeq,
    };
    for (const listener of this.#gapListeners) {
      try {
        listener(gap);
      } catch {
        // Handlers have no logger available; swallow so one throwing
        // listener doesn't break delivery to the others.
      }
    }
  }

  #deliver(value: T, meta: ChannelMeta): void {
    for (const listener of this.#listeners) {
      try {
        listener(value, meta);
      } catch {
        // Swallow — a throwing listener must not corrupt sequence tracking
        // or block delivery to the remaining listeners, and there is no
        // logger available to a handler.
      }
    }
  }
}
