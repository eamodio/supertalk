/**
 * @fileoverview AbortSignal handler for cancellation across connection boundaries.
 *
 * Teaches Supertalk how to serialize AbortSignal across the wire. Since
 * AbortSignal is an event target (non-cloneable), the handler creates a proxy
 * relationship: the sender's signal gets an ID, and the receiver gets a fresh
 * AbortController's signal keyed by that ID.
 *
 * The handler uses two message types:
 * - `abort`: Real cancellation — the sender's signal was aborted
 * - `release`: Cleanup — the sender is done with the signal (e.g., call completed)
 *
 * Convention: `controller.abort('completed')` on the sender side triggers a
 * `release` message instead of `abort`. This lets the receiver distinguish
 * between "the work was cancelled" and "the work finished, clean up the signal."
 *
 * @example
 * ```ts
 * import {expose, wrap} from '@supertalk/core';
 * import {AbortSignalHandler} from '@supertalk/core/handlers/abort-signal.js';
 *
 * // Both sides need compatible handlers
 * const options = {handlers: [new AbortSignalHandler()]};
 *
 * expose(service, endpoint, options);
 * const remote = await wrap<Service>(endpoint, options);
 *
 * // Caller side
 * const controller = new AbortController();
 * const result = remote.search('query', controller.signal);
 * // Cancel:
 * controller.abort();
 * // Or signal completion (cleanup only):
 * controller.abort('completed');
 * ```
 */

import {WIRE_TYPE} from '../lib/constants.js';
import type {
  Handler,
  HandlerConnectionContext,
  ToWireContext,
} from '../lib/types.js';

const ABORT_SIGNAL_WIRE_TYPE = 'abort-signal';

/**
 * Reason value that signals completion rather than cancellation.
 * When `controller.abort('completed')` is called, the handler sends a
 * `release` message instead of `abort`.
 */
export const COMPLETED = 'completed';

/**
 * Reason value used when the connection is disconnected.
 */
const DISCONNECTED = 'disconnected';

interface WireAbortSignal {
  [WIRE_TYPE]: typeof ABORT_SIGNAL_WIRE_TYPE;
  id: number;
  aborted: boolean;
  reason?: unknown;
}

interface AbortMessage {
  type: 'abort';
  id: number;
  reason?: unknown;
}

interface ReleaseMessage {
  type: 'release';
  id: number;
}

type HandlerPayload = AbortMessage | ReleaseMessage;

/**
 * Handler for AbortSignal serialization across connection boundaries.
 *
 * Create one instance per side and include in the `handlers` array for both
 * `expose()` and `wrap()`.
 */
export class AbortSignalHandler implements Handler<
  AbortSignal,
  WireAbortSignal
> {
  readonly wireType = ABORT_SIGNAL_WIRE_TYPE;

  #ctx: HandlerConnectionContext | undefined;

  // Sender side: signals we've sent, keyed by ID
  #nextId = 1;
  #signalToId = new WeakMap<AbortSignal, number>();
  #sentSignals = new Map<number, AbortSignal>();
  #abortListeners = new Map<number, () => void>();

  // Receiver side: AbortControllers we've created, keyed by ID
  #receivedControllers = new Map<number, AbortController>();

  canHandle(value: unknown): value is AbortSignal {
    return value instanceof AbortSignal;
  }

  toWire(signal: AbortSignal, _ctx: ToWireContext): WireAbortSignal {
    // If already aborted, send the aborted state directly — no need to track
    if (signal.aborted) {
      return {
        [WIRE_TYPE]: ABORT_SIGNAL_WIRE_TYPE,
        id: 0, // No tracking needed
        aborted: true,
        reason: signal.reason,
      };
    }

    // Check if we've already sent this signal
    let id = this.#signalToId.get(signal);
    if (id !== undefined) {
      return {
        [WIRE_TYPE]: ABORT_SIGNAL_WIRE_TYPE,
        id,
        aborted: false,
      };
    }

    // New signal — assign an ID and start listening
    id = this.#nextId++;
    this.#signalToId.set(signal, id);
    this.#sentSignals.set(id, signal);

    const listener = () => {
      if (signal.reason === COMPLETED) {
        this.#ctx?.sendMessage({type: 'release', id} satisfies ReleaseMessage);
      } else {
        this.#ctx?.sendMessage({
          type: 'abort',
          id,
          reason: signal.reason,
        } satisfies AbortMessage);
      }
      this.#cleanupSent(id);
    };

    signal.addEventListener('abort', listener, {once: true});
    this.#abortListeners.set(id, listener);

    return {
      [WIRE_TYPE]: ABORT_SIGNAL_WIRE_TYPE,
      id,
      aborted: false,
    };
  }

  fromWire(wire: WireAbortSignal): AbortSignal {
    // Already-aborted signal — return a pre-aborted signal
    if (wire.aborted) {
      return AbortSignal.abort(wire.reason);
    }

    // Reuse existing controller if the same signal was sent before
    const existing = this.#receivedControllers.get(wire.id);
    if (existing !== undefined) {
      return existing.signal;
    }

    // Create a new controller for this ID
    const controller = new AbortController();
    this.#receivedControllers.set(wire.id, controller);
    return controller.signal;
  }

  connect(ctx: HandlerConnectionContext): void {
    this.#ctx = ctx;
  }

  onMessage(payload: unknown): void {
    const msg = payload as HandlerPayload;
    if (msg.type === 'abort') {
      this.#receivedControllers.get(msg.id)?.abort(msg.reason);
      this.#receivedControllers.delete(msg.id);
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    } else if (msg.type === 'release') {
      this.#receivedControllers.delete(msg.id);
    }
  }

  disconnect(): void {
    this.#ctx = undefined;

    // Abort all received controllers with 'disconnected' reason
    for (const controller of this.#receivedControllers.values()) {
      controller.abort(DISCONNECTED);
    }
    this.#receivedControllers.clear();

    // Remove all abort listeners from sent signals
    for (const [id, listener] of this.#abortListeners) {
      const signal = this.#sentSignals.get(id);
      signal?.removeEventListener('abort', listener);
    }
    this.#abortListeners.clear();
    this.#sentSignals.clear();
    // Reset WeakMap and ID counter so reuse after reconnect doesn't
    // find stale IDs that skip listener registration.
    this.#signalToId = new WeakMap();
    this.#nextId = 1;
  }

  #cleanupSent(id: number): void {
    this.#abortListeners.delete(id);
    this.#sentSignals.delete(id);
    // WeakMap entry will be GC'd automatically
  }

  /**
   * Get the number of sent signals being tracked (for testing).
   * @internal
   */
  get _sentCount(): number {
    return this.#sentSignals.size;
  }

  /**
   * Get the number of received controllers being tracked (for testing).
   * @internal
   */
  get _receivedCount(): number {
    return this.#receivedControllers.size;
  }
}
