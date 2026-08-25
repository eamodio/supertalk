/**
 * Connection class - manages state and communication for both sides of a
 * Supertalk connection.
 *
 * This file contains:
 * - Connection class with proxy lifecycle management
 * - Wire value serialization/deserialization (#toWire/#fromWire)
 * - Message handling and dispatch
 * - Proxy creation and release tracking
 *
 * @fileoverview Core connection implementation.
 */

import type {
  Endpoint,
  Message,
  BatchMessage,
  CallMessage,
  WireValue,
  WireProxyProperty,
  WireThrown,
  Options,
  Logger,
  ProxyPropertyMetadata,
  Handler,
  ToWireContext,
  FromWireContext,
  SerializedError,
  WireProxy,
} from './types.js';
import {isWireProxy, isWirePromise} from './types.js';
import {
  PROXY_PROPERTY_BRAND,
  PROXY_VALUE,
  WIRE_TYPE,
  HANDSHAKE_ID,
  NON_CLONEABLE,
  NOTIFY_ID,
  INTERNAL,
} from './constants.js';
import {
  isProxyMarker,
  isOpaqueMarker,
  isProxyProperty,
  isPromise,
  isTransferMarker,
  serializeError,
  deserializeError,
  NonCloneableError,
  ConnectionClosedError,
  proxy,
} from './protocol.js';

/**
 * Allocate a session id — an instance-unique token, not a counter. A peer
 * recreated as a brand-new Connection over a persistent endpoint (a webview
 * reloading, a worker respawning) would restart a counter at the same value,
 * and calls tagged with it would sail through the receiver's staleness check.
 * The randomness is for collision avoidance, not security.
 */
const newSessionId = (): number => Math.floor(Math.random() * 2 ** 52);

/**
 * Anchors the proxy a property callable was read off. Symbol-keyed so it is
 * invisible to structured clone, `Object.keys`, and the proxy-property
 * serialization at #processForClone. See #createProxyProperty.
 */
const RETAINED_PROXY = Symbol('retainedProxy');

/**
 * Inner cache key standing in for an unknown owner session — a proxy from a
 * peer that predates the `s` wire field, or one received before any
 * handshake. Real sessions are non-negative, so this can never collide.
 */
const UNKNOWN_SESSION = -1;

/**
 * Pending call waiting for a response.
 */
interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

/**
 * A callable function that is also thenable.
 * Enables both `await proxy.method(args)` and `await proxy.property`.
 */
interface ProxyProperty {
  (...args: Array<unknown>): Promise<unknown>;
  then<TResult1 = unknown, TResult2 = never>(
    onfulfilled?: ((value: unknown) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2>;
  [PROXY_PROPERTY_BRAND]: ProxyPropertyMetadata;
}

/**
 * Unified connection state and logic for Supertalk.
 *
 * Both sides of a connection use the same Connection class.
 * The only difference is initialization:
 * - expose() side: registers a root service object
 * - wrap() side: returns a proxy for the root service
 */
export class Connection {
  #endpoint: Endpoint;
  #nestedProxies: boolean;
  #debug: boolean;
  #logger: Logger | undefined;
  #handlers: Array<Handler>;
  #handlersByWireType = new Map<string, Handler>();

  #nextId = 0;
  #idStep = 1;
  #closed = false;

  // Root proxy from the most recent successful handshake, and the callbacks
  // to fan out to on every handshake (subscription bookkeeping lives in
  // lib/subscription.ts; this is just the hook).
  #root: object | undefined;
  // Every proxy that has ever been the handshake root, across sessions —
  // lets subscribe() tell a (possibly stale) root from a nested proxy after
  // reset() has cleared the registries.
  #rootProxies = new WeakSet<object>();
  #readyCbs = new Set<(root: object) => void>();
  #closedCbs = new Set<() => void>();

  // Local object registry (strong refs) - objects we expose to remote
  #localById = new Map<number, object>();
  #localByObject = new WeakMap<object, number>();

  // Remote proxy cache (weak refs), keyed by id and then by owner session.
  // Identity is per (owner session, id): the peer can reclaim an id for an
  // unrelated object after a reset(), so each owner session gets its own
  // proxy and they coexist. The root (id 0) is the one exception — it names a
  // role rather than an object, and is re-keyed in place (see
  // #createRemoteProxy). Entries must coexist rather than replace one
  // another because a stale call's arguments are deserialized before the call
  // is rejected (see #handleCall): a stale arg naming a reused id reaches
  // this cache, and must not be able to evict the live proxy for it.
  // The key UNKNOWN_SESSION stands in for an owner session we don't know —
  // a peer that predates the `s` wire field, or a proxy seen before any
  // handshake — and outgoing calls through such a proxy omit the wire field.
  #remoteProxyById = new Map<number, Map<number, WeakRef<object>>>();
  #remoteProxyByObject = new WeakMap<object, number>();
  // The owner session frozen onto each remote proxy, and the tag every call
  // through it carries. This is not a duplicate of #remoteProxyById's key:
  // that map answers "which proxy is current for this (owner, id)", while a
  // retained *stale* proxy still has to know its own owner session at send
  // time, and it is not reachable from the cache by id — a newer proxy for
  // the same id holds its own key. Reading it here needs no lookup and cannot
  // be confused by a successor. The root's entry is rewritten when it is
  // re-keyed; see #createRemoteProxy.
  #remoteProxyOwnerSession = new WeakMap<object, number | undefined>();
  #remoteCleanup: FinalizationRegistry<{
    id: number;
    session: number;
    ownerKey: number;
  }>;
  #sessionId = newSessionId();
  // The peer's session id, learned from its handshake `return` frame. Owner
  // sessions ride on the wire proxies themselves, so this is not what tags
  // outgoing calls; it has two narrow jobs: the fallback owner session for a
  // peer that predates the `s` wire field, and the trigger for re-keying a
  // retained root when the peer re-handshakes. undefined until the first
  // handshake is received (or after reset()/close(), until the next one).
  #peerSession: number | undefined;

  // Pending RPC calls awaiting response
  #pendingCalls = new Map<number, PendingCall>();

  // Promise tracking
  #pendingRemotePromises = new Map<number, PendingCall>();

  // Batching state
  #batchingEnabled: boolean;
  #queue: Array<{
    message: unknown;
    transfers: Array<Transferable> | undefined;
  }> = [];
  #flushScheduled = false;

  constructor(endpoint: Endpoint, options: Options = {}) {
    this.#endpoint = endpoint;
    this.#nestedProxies = options.nestedProxies ?? false;
    this.#debug = options.debug ?? false;
    this.#logger = options.logger;
    this.#handlers = options.handlers ?? [];
    this.#batchingEnabled = options.batching ?? false;

    // Build handler lookup map and call connect() on handlers that support it
    for (const handler of this.#handlers) {
      this.#handlersByWireType.set(handler.wireType, handler);

      // Call connect() if the handler supports messaging
      if (typeof handler.connect === 'function') {
        handler.connect({
          sendMessage: (payload: unknown): void => {
            this.#sendHandlerMessage(handler.wireType, payload);
          },
        });
      }
    }

    // Set up finalization registry to notify remote when proxies are GC'd.
    // The held value includes a session ID so stale finalizers from before a
    // reset() are ignored (IDs restart from 0 after reset, so a stale release
    // could otherwise match a newly allocated ID).
    this.#remoteCleanup = new FinalizationRegistry(
      ({
        id,
        session,
        ownerKey,
      }: {
        id: number;
        session: number;
        ownerKey: number;
      }) => {
        if (session !== this.#sessionId) return;
        const byOwner = this.#remoteProxyById.get(id);
        const ref = byOwner?.get(ownerKey);
        // Nothing under this exact key: either an earlier finalizer for the
        // same (owner session, id) already cleaned up and released, or the
        // root was re-keyed — which re-registers the surviving proxy, so that
        // registration owns its release.
        if (byOwner === undefined || ref === undefined) return;
        // A live object under this exact key means the same (owner, id) was
        // re-minted after this proxy died; that newer registration owns the
        // release, and sending one now would unregister its target.
        if (ref.deref() !== undefined) return;
        byOwner.delete(ownerKey);
        if (byOwner.size === 0) this.#remoteProxyById.delete(id);
        // ownerKey is the owner session this proxy named. Tagging is what
        // stops a release crossing a peer reset from unregistering whatever
        // unrelated object has since reclaimed the id.
        this.#post({
          type: 'release',
          id,
          ...(ownerKey !== UNKNOWN_SESSION && {session: ownerKey}),
        });
      },
    );

    // #onMessage must be an arrow field (not a method) so the same function
    // reference is used for both addEventListener and removeEventListener.
    endpoint.addEventListener('message', this.#onMessage);
  }

  #post(message: unknown, transfer?: Array<Transferable>): void {
    if (this.#closed) return;
    if (!this.#batchingEnabled) {
      this.#endpoint.postMessage(message, transfer);
      return;
    }
    this.#queue.push({message, transfers: transfer});
    if (!this.#flushScheduled) {
      this.#flushScheduled = true;
      queueMicrotask(() => this.#flush());
    }
  }

  #flush(): void {
    this.#flushScheduled = false;
    const queue = this.#queue;
    this.#queue = [];
    if (queue.length === 0) return;

    // If postMessage throws (e.g. structured-clone failure), reject any pending
    // calls whose messages are in this batch so callers don't hang indefinitely.
    const rejectFlushed = (error: unknown): void => {
      this.#logger?.error?.('Failed to post message', error);
      const err = error instanceof Error ? error : new Error(String(error));
      for (const {message} of queue) {
        const msg = message as {type?: string; id?: number};
        if (msg.type === 'call' && msg.id !== undefined) {
          const pending = this.#pendingCalls.get(msg.id);
          if (pending) {
            this.#pendingCalls.delete(msg.id);
            pending.reject(err);
          }
        }
      }
    };

    if (queue.length === 1) {
      // Single message: send directly, no batch wrapper
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const {message, transfers} = queue[0]!;
      try {
        this.#endpoint.postMessage(message, transfers);
      } catch (error) {
        rejectFlushed(error);
      }
    } else {
      // Multiple messages: wrap in batch, merge transfer lists
      const allTransfers: Array<Transferable> = [];
      const messages: Array<unknown> = [];
      for (const {message, transfers} of queue) {
        messages.push(message);
        if (transfers) allTransfers.push(...transfers);
      }
      try {
        this.#endpoint.postMessage(
          {type: 'batch', messages},
          allTransfers.length > 0 ? allTransfers : undefined,
        );
      } catch (error) {
        rejectFlushed(error);
      }
    }
  }

  /**
   * Send a handler message to the remote side.
   */
  #sendHandlerMessage(wireType: string, payload: unknown): void {
    const transfers: Array<Transferable> = [];
    this.#post(
      {
        type: 'handler',
        wireType,
        payload: this.#toWire(payload, '', transfers),
      },
      transfers,
    );
  }

  /**
   * Allocate the next unique ID for this side of the connection.
   * After side selection (expose/wrap), IDs increment by 2 to ensure
   * the expose side uses even IDs and the wrap side uses odd IDs.
   */
  #allocId(): number {
    const id = this.#nextId;
    this.#nextId += this.#idStep;
    return id;
  }

  /**
   * Expose an object as the root service and send the ready signal.
   */
  expose(obj: object): void {
    // Use even IDs (0, 2, 4, ...) on the expose side
    this.#idStep = 2;
    this.#registerLocal(obj);
    this.#post({
      type: 'return',
      id: HANDSHAKE_ID,
      value: this.#makeProxyWire(obj),
      session: this.#sessionId,
    });
  }

  /**
   * Drop unsent batched messages and reject all pending calls/promises with
   * a `ConnectionClosedError` carrying the given reason.
   */
  #dropPendingWork(reason: 'closed' | 'reset'): void {
    this.#queue = [];
    this.#flushScheduled = false;
    const err = new ConnectionClosedError(reason);
    for (const {reject} of this.#pendingCalls.values()) {
      reject(err);
    }
    this.#pendingCalls.clear();
    for (const {reject} of this.#pendingRemotePromises.values()) {
      reject(err);
    }
    this.#pendingRemotePromises.clear();
  }

  /**
   * Close the connection and stop listening for messages.
   */
  close(): void {
    this.#closed = true;
    this.#dropPendingWork('closed');
    this.#endpoint.removeEventListener('message', this.#onMessage);

    // Clear the cached root so a later _onReady registration doesn't fire
    // synchronously against a dead session, and notify closed-callbacks so a
    // pending subscription `ready` settles instead of hanging.
    this.#root = undefined;
    // A closed connection must not tag outgoing calls with a peer session
    // learned before teardown — the next session's handshake is what
    // re-establishes it.
    this.#peerSession = undefined;
    for (const cb of this.#closedCbs) {
      try {
        cb();
      } catch (error) {
        this.#logger?.error?.('Error in _onClosed callback', error);
      }
    }

    // Call disconnect() on handlers that support it
    for (const handler of this.#handlers) {
      handler.disconnect?.();
    }
  }

  /**
   * Enables `using connection = new Connection(...)` to close the
   * connection automatically when the scope ends.
   */
  [Symbol.dispose](): void {
    this.close();
  }

  /**
   * Reset the connection for reuse with a new peer.
   *
   * Rejects pending operations, clears all registries, cycles handlers
   * (disconnect + reconnect), and resets ID allocation. After reset(),
   * call expose() or waitForReady() to start a new session.
   *
   * Optionally accepts a new endpoint to replace the current one.
   * If the connection was previously closed, the message listener is
   * re-added to the (new or existing) endpoint.
   */
  reset(endpoint?: Endpoint): void {
    this.#dropPendingWork('reset');

    // Clear the cached root — subscriptions must wait for the next ready
    // rather than re-issuing against a dead root.
    this.#root = undefined;

    // Clear local object registries
    this.#localById.clear();
    // WeakMap has no .clear() — reassign to drop all entries
    this.#localByObject = new WeakMap();

    // Clear remote proxy cache
    this.#remoteProxyById.clear();
    // WeakMap has no .clear() — reassign to drop all entries
    this.#remoteProxyByObject = new WeakMap();
    // Take a fresh session token to invalidate any outstanding
    // FinalizationRegistry callbacks from the previous session. IDs restart
    // from 0, so without this a stale finalizer could release a newly
    // allocated object with the same ID.
    this.#sessionId = newSessionId();
    // Forget the peer's session too — it's only known from a handshake we've
    // already received, and a new session must not tag its calls with a
    // belief carried over from before the reset. The next handshake sets it
    // again.
    this.#peerSession = undefined;

    // Reset ID allocation
    this.#nextId = 0;
    this.#idStep = 1;

    // Disconnect handlers before touching the endpoint or reopening the
    // connection, so they can't send messages during the transition.
    for (const handler of this.#handlers) {
      handler.disconnect?.();
    }

    // Swap endpoint / re-attach listener before opening the connection.
    // The connection is still #closed at this point, so incoming messages
    // are not processed until we set #closed = false below.
    if (endpoint !== undefined && endpoint !== this.#endpoint) {
      // Swap endpoint: remove from old (if not already closed), add to new
      if (!this.#closed) {
        this.#endpoint.removeEventListener('message', this.#onMessage);
      }
      this.#endpoint = endpoint;
      endpoint.addEventListener('message', this.#onMessage);
    } else if (this.#closed) {
      // Same endpoint but was closed — re-add listener
      this.#endpoint.addEventListener('message', this.#onMessage);
    }
    // If not closed and same endpoint: listener is already attached, leave it

    // Open the connection before reconnecting handlers, so any messages sent
    // during handler.connect() are not silently dropped by #post().
    this.#closed = false;

    // Reconnect handlers now that the connection is fully open and pointing at
    // the correct endpoint.
    for (const handler of this.#handlers) {
      if (typeof handler.connect === 'function') {
        handler.connect({
          sendMessage: (payload: unknown): void => {
            this.#sendHandlerMessage(handler.wireType, payload);
          },
        });
      }
    }
  }

  #assertSession(session: number): void {
    if (session !== this.#sessionId) {
      throw new Error('Stale proxy from previous session');
    }
  }

  /**
   * Wait for the ready signal from the remote side.
   * Returns a proxy for the root service.
   */
  waitForReady(): Promise<unknown> {
    // Use odd IDs (1, 3, 5, ...) on the wrap side.
    // This ensures local IDs never collide with the expose side's even IDs.
    this.#nextId = 1;
    this.#idStep = 2;
    const {promise, resolve, reject} = Promise.withResolvers<unknown>();
    this.#pendingCalls.set(HANDSHAKE_ID, {resolve, reject});
    return promise.then((root) => {
      // Record the root and fan out to registered subscriptions before
      // handing the root back to the caller.
      this.#root = root as object;
      this.#rootProxies.add(this.#root);
      for (const cb of this.#readyCbs) {
        try {
          cb(this.#root);
        } catch (error) {
          this.#logger?.error?.('Error in _onReady callback', error);
        }
      }
      return root;
    });
  }

  /**
   * Register `cb` to run with the root proxy on every successful handshake
   * (including after a `reset()` + `waitForReady()` reconnect). Invoked
   * synchronously if the root is already available.
   * @internal
   */
  _onReady(cb: (root: object) => void): () => void {
    this.#readyCbs.add(cb);
    if (this.#root !== undefined) {
      cb(this.#root);
    }
    return () => {
      this.#readyCbs.delete(cb);
    };
  }

  /**
   * Register `cb` to run when the connection is closed via `close()`.
   * Invoked immediately if the connection is already closed. Not fired by
   * `reset()` — a reset is a reconnect, not a teardown.
   * @internal
   */
  _onClosed(cb: () => void): () => void {
    this.#closedCbs.add(cb);
    if (this.#closed) {
      cb();
    }
    return () => {
      this.#closedCbs.delete(cb);
    };
  }

  /**
   * Report an error through the configured `logger`, if any.
   * @internal
   */
  _logError(message: string, error: unknown): void {
    this.#logger?.error?.(message, error);
  }

  // ============================================================
  // Local object registry (strong refs, we expose to remote)
  // ============================================================

  /**
   * Register a local object and return its ID.
   */
  #registerLocal(obj: object): number {
    let id = this.#localByObject.get(obj);
    if (id !== undefined) {
      return id;
    }
    id = this.#allocId();
    this.#localById.set(id, obj);
    this.#localByObject.set(obj, id);
    return id;
  }

  /**
   * Get a local object by its ID.
   */
  #getLocal(id: number): object | undefined {
    return this.#localById.get(id);
  }

  // ============================================================
  // Remote proxy/handle cache (weak refs)
  // ============================================================

  /**
   * Get the remote ID for a proxy, if it exists.
   */
  #getRemoteProxyId(obj: object): number | undefined {
    return this.#remoteProxyByObject.get(obj);
  }

  /**
   * Get the remote ID for a proxy, if it exists.
   * @internal
   */
  _proxyId(obj: object): number | undefined {
    return this.#getRemoteProxyId(obj);
  }

  /**
   * The current session id — captured by `notify()` so a notifier from a
   * previous session (before a `reset()`) fails like a stale proxy instead
   * of silently targeting a reused id.
   * @internal
   */
  get _session(): number {
    return this.#sessionId;
  }

  /**
   * Whether `obj` was the root proxy of any handshake on this connection.
   * Membership survives `reset()` — the registries are cleared but the
   * WeakSet is not — so a retained pre-reset root is still recognized as a
   * root (and a stale nested proxy is still recognized as nested).
   * @internal
   */
  _isRootProxy(obj: object): boolean {
    return this.#rootProxies.has(obj);
  }

  // ============================================================
  // Wire value serialization
  // ============================================================

  /**
   * Serialize a value for transmission.
   */
  #toWire(
    value: unknown,
    path: string,
    transfers: Array<Transferable>,
  ): WireValue {
    // Handle top-level-only wire types
    if (isProxyProperty(value)) {
      return {
        [WIRE_TYPE]: 'property',
        ...value[PROXY_PROPERTY_BRAND],
      };
    }
    // Delegate all other value handling to processForClone
    // Pass a Map to track cycles when recursing in debug/nested mode
    return this.#processForClone(value, path, transfers, new Map());
  }

  /**
   * Create a WireProxy for a value.
   */
  #makeProxyWire(value: object, opaque = false): WireProxy {
    const remoteId = this.#getRemoteProxyId(value);
    if (remoteId !== undefined) {
      // Echoing a proxy back to the side that owns it: carry the owner
      // session this proxy was received with, not ours — the target lives in
      // the peer's session, not this one. Absent for a proxy from a peer
      // that predates the field.
      const ownerSession = this.#remoteProxyOwnerSession.get(value);
      return {
        [WIRE_TYPE]: 'proxy',
        id: remoteId,
        o: opaque,
        ...(ownerSession !== undefined && {s: ownerSession}),
      };
    }
    return {
      [WIRE_TYPE]: 'proxy',
      id: this.#registerLocal(value),
      o: opaque,
      s: this.#sessionId,
    };
  }

  /**
   * Process a value for wire serialization.
   * Handles markers, recursion, and debug mode errors.
   * @param seen - Map tracking visited objects to their processed results (for cycle detection)
   * @param callId - If set, the outgoing call ID; passed to handlers via ToWireContext
   */
  #processForClone(
    value: unknown,
    path: string,
    transfers: Array<Transferable>,
    seen: Map<object, unknown>,
    callId?: number,
  ): unknown {
    // Null and primitives are sent directly
    if (
      value == null ||
      (typeof value !== 'object' && typeof value !== 'function')
    ) {
      return value;
    }

    // Check for cycles - return cached result if we've seen this object.
    // This must happen before any other object processing to avoid:
    // - Creating duplicate wire proxies for the same object
    // - Registering the same promise multiple times with different IDs
    const cached = seen.get(value);
    if (cached !== undefined) {
      return cached;
    }

    // Transfer markers: add to transfer list and return raw value
    if (isTransferMarker(value)) {
      if (path && this.#debug && !this.#nestedProxies) {
        throw new NonCloneableError('transfer', path);
      }
      // Dedup: the same underlying Transferable wrapped in two separate
      // transfer() markers would cause a DataCloneError from postMessage.
      if (!transfers.includes(value.value)) {
        transfers.push(value.value);
      }
      seen.set(value, value.value);
      return value.value;
    }

    // Proxy markers - extract the underlying value
    if (isProxyMarker(value)) {
      if (path && this.#debug && !this.#nestedProxies) {
        throw new NonCloneableError('proxy', path);
      }
      const wire = this.#makeProxyWire(
        (value as {[PROXY_VALUE]: object})[PROXY_VALUE],
        isOpaqueMarker(value),
      );
      seen.set(value as object, wire);
      return wire;
    }

    // Functions are always proxied (or throw in debug-only mode for nested fns)
    if (typeof value === 'function') {
      if (path && this.#debug && !this.#nestedProxies) {
        throw new NonCloneableError('function', path);
      }
      const wire = this.#makeProxyWire(value as object);
      seen.set(value as object, wire);
      return wire;
    }

    // Check if this is a proxy we received from remote
    if (this.#getRemoteProxyId(value) !== undefined) {
      const wire = this.#makeProxyWire(value, '__o' in value);
      seen.set(value, wire);
      return wire;
    }

    // Promises are proxied (or throw in debug-only mode for nested promises)
    if (isPromise(value)) {
      if (path && this.#debug && !this.#nestedProxies) {
        throw new NonCloneableError('promise', path);
      }
      const wire = {[WIRE_TYPE]: 'promise', id: this.#registerPromise(value)};
      seen.set(value, wire);
      return wire;
    }

    // Check handlers
    if (this.#handlers.length > 0) {
      for (const handler of this.#handlers) {
        if (handler.canHandle(value)) {
          const ctx: ToWireContext = {
            toWire: (v: unknown, key?: string): WireValue => {
              const p = key ? (path ? `${path}.${key}` : key) : path;
              return this.#processForClone(v, p, transfers, seen, callId);
            },
            ...(callId !== undefined && {callId}),
          };
          const wire = handler.toWire(value, ctx);
          seen.set(value, wire);
          return wire;
        }
      }
    }

    // Decide whether to recurse into arrays/objects
    const shouldRecurse = this.#nestedProxies || this.#debug;

    if (!shouldRecurse) {
      return value;
    }

    if (Array.isArray(value)) {
      const processed: Array<unknown> = [];
      seen.set(value, processed); // Cache before recursing to handle cycles
      for (let i = 0; i < value.length; i++) {
        processed.push(
          this.#processForClone(
            value[i],
            `${path}[${String(i)}]`,
            transfers,
            seen,
            callId,
          ),
        );
      }
      return processed;
    }

    // Only recurse into plain objects (prototype is Object.prototype or null).
    // All other objects (Map, Set, Date, class instances, etc.) pass through
    // for structured clone to handle natively.

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const proto = Object.getPrototypeOf(value);
    if (proto === Object.prototype || proto === null) {
      const processed: Record<string, unknown> = {};
      seen.set(value, processed); // Cache before recursing to handle cycles
      for (const key of Object.keys(value)) {
        processed[key] = this.#processForClone(
          (value as Record<string, unknown>)[key],
          path ? `${path}.${key}` : key,
          transfers,
          seen,
          callId,
        );
      }
      return processed;
    }

    return value;
  }

  // ============================================================
  // Wire value deserialization
  // ============================================================

  /**
   * Create a FromWireContext that shares a seen map for cycle detection.
   */
  #makeFromWireContext(seen: Map<object, unknown>): FromWireContext {
    return {
      fromWire: (wire: WireValue): unknown =>
        this.#processFromClone(wire, seen),
    };
  }

  /**
   * Deserialize a value from wire format.
   */
  #fromWire(wire: WireValue): unknown {
    // Handle top-level-only wire types first
    const wireType = (wire as Record<string, unknown> | null)?.[WIRE_TYPE];
    if (wireType === 'property') {
      const pp = wire as WireProxyProperty;
      const target = this.#getLocal(pp.targetProxyId);
      if (!target) {
        throw new ReferenceError(
          `Proxy property target ${String(pp.targetProxyId)} not found`,
        );
      }
      return (target as Record<string, unknown>)[pp.property];
    }
    if (wireType === 'thrown') {
      throw deserializeError((wire as WireThrown).error);
    }

    // Delegate all other wire value handling to processFromClone
    // Pass a Map to track cycles when recursing in nested mode
    return this.#processFromClone(wire, new Map());
  }

  /**
   * Process a value from wire format, handling markers and recursion.
   * @param seen - Map tracking visited objects to their processed results (for cycle detection)
   */
  #processFromClone(value: unknown, seen: Map<object, unknown>): unknown {
    if (value === null || typeof value !== 'object') {
      return value;
    }

    // Check for cycles - return cached result if we've seen this object.
    // This must happen before any other object processing.
    const cached = seen.get(value);
    if (cached !== undefined) {
      return cached;
    }

    if (isWireProxy(value)) {
      // A peer that predates `s` sends none; fall back to the session learned
      // from the handshake.
      const ownerSession = value.s ?? this.#peerSession;
      const local = this.#getLocal(value.id);
      // An echo of one of our own ids resolves to the live local object only
      // if it was minted in our current session. After a reset() that id may
      // hold an unrelated object, so a stale echo must not resolve to it —
      // fall through and build a remote proxy carrying the stale owner
      // session instead. Calling through that proxy fails cleanly against
      // the peer (unknown target, or rejected as stale) rather than silently
      // invoking the wrong local object.
      if (
        local !== undefined &&
        (value.s === undefined || value.s === this.#sessionId)
      ) {
        const result = proxy(local, value.o);
        seen.set(value, result);
        return result;
      }
      const result = this.#createRemoteProxy(value.id, value.o, ownerSession);
      seen.set(value, result);
      return result;
    }

    if (isWirePromise(value)) {
      const {promise, resolve, reject} = Promise.withResolvers<unknown>();
      this.#pendingRemotePromises.set(value.id, {resolve, reject});
      seen.set(value, promise);
      return promise;
    }

    // Check handler wireTypes
    const wireType = (value as Record<string, unknown>)[WIRE_TYPE];
    if (typeof wireType === 'string') {
      const handler = this.#handlersByWireType.get(wireType);
      if (handler?.fromWire) {
        const result = handler.fromWire(value, this.#makeFromWireContext(seen));
        seen.set(value, result);
        return result;
      }
    }

    // Only recurse if nestedProxies enabled
    if (!this.#nestedProxies) {
      return value;
    }

    if (Array.isArray(value)) {
      const processed: Array<unknown> = [];
      seen.set(value, processed); // Cache before recursing to handle cycles
      for (const item of value) {
        processed.push(this.#processFromClone(item, seen));
      }
      return processed;
    }

    // Only recurse into plain objects. This protects:
    // - Built-in types preserved by structured clone (ReadableStream, etc.)
    // - Objects returned by handlers (which may be class instances)
    if (Object.getPrototypeOf(value) !== Object.prototype) {
      return value;
    }

    const processed: Record<string, unknown> = {};
    seen.set(value, processed); // Cache before recursing to handle cycles
    for (const key of Object.keys(value)) {
      processed[key] = this.#processFromClone(
        (value as Record<string, unknown>)[key],
        seen,
      );
    }
    return processed;
  }

  // ============================================================
  // Promise handling
  // ============================================================

  /**
   * Register a local promise for sending to remote.
   */
  #registerPromise(promise: Promise<unknown>): number {
    const id = this.#allocId();
    promise.then(
      (value) => {
        try {
          const transfers: Array<Transferable> = [];
          const wire = this.#toWire(value, '', transfers);
          this.#post(
            {
              type: 'resolve',
              id,
              value: wire,
            },
            transfers,
          );
        } catch {
          // Serialization or postMessage failed for the resolved value.
          // Send a reject so the remote side doesn't hang.
          this.#post({
            type: 'reject',
            id,
            error: serializeError(
              new Error('Failed to serialize resolved promise value'),
            ),
          });
        }
      },
      (error: unknown) => {
        try {
          this.#post({
            type: 'reject',
            id,
            error: serializeError(error),
          });
        } catch {
          // Nothing more we can do — the reject message itself failed to send.
          // The remote side's pending promise will be cleaned up on close/reset.
        }
      },
    );
    return id;
  }

  // ============================================================
  // Remote proxy creation
  // ============================================================

  /**
   * Create a proxy for a remote object.
   * Opaque proxies are simple objects (no JS Proxy overhead).
   */
  #createRemoteProxy(
    id: number,
    opaque: boolean | undefined,
    ownerSession: number | undefined,
  ): object {
    const byOwner = this.#remoteProxyById.get(id);
    if (byOwner !== undefined) {
      if (ownerSession === undefined) {
        // A peer that sends no `s` has only ever had one meaning for this id,
        // so whichever entry is still live is the right one.
        for (const ref of byOwner.values()) {
          const cached = ref.deref();
          if (cached !== undefined) return cached;
        }
      } else {
        const cached = byOwner.get(ownerSession)?.deref();
        // Exact (owner session, id) hit: the same object as before.
        if (cached !== undefined) return cached;
        if (id === HANDSHAKE_ID && ownerSession === this.#peerSession) {
          // The root is a role, not an object. Id 0 is always and only the
          // expose-side root — expose() registers it first, and the wrap
          // side's own locals are odd — so a new owner session here means
          // "the peer's root service" moved to a fresh object under the same
          // role. Re-key the object that actually holds the role — #root —
          // rather than scanning for a live entry: isolated id-0 proxies for
          // superseded sessions coexist in this map by design (the check
          // above mints them), so a scan can promote one of those into the
          // role and leave the real root frozen at its old session.
          // Only the peer's CURRENT session may claim the role: a stale
          // frame naming id 0 under a superseded session must not re-key a
          // live root backwards, so it falls through and mints an isolated
          // proxy that fails cleanly instead. (The handshake records
          // #peerSession before deserializing its value, so the legitimate
          // re-key always passes this check.)
          const held = this.#root;
          if (
            held !== undefined &&
            this.#remoteProxyByObject.get(held) === id
          ) {
            const heldKey =
              this.#remoteProxyOwnerSession.get(held) ?? UNKNOWN_SESSION;
            const ref = byOwner.get(heldKey) ?? new WeakRef(held);
            byOwner.delete(heldKey);
            byOwner.set(ownerSession, ref);
            this.#remoteProxyOwnerSession.set(held, ownerSession);
            // Re-key the finalizer registration too, so its held ownerKey
            // still matches the entry it is responsible for releasing.
            this.#remoteCleanup.unregister(held);
            this.#remoteCleanup.register(
              held,
              {id, session: this.#sessionId, ownerKey: ownerSession},
              held,
            );
            return held;
          }
          // No retained root — the initial handshake, or a root only ever
          // seen as a call argument. Fall through and mint the canonical
          // root fresh; any isolated id-0 proxies stay frozen at their own
          // keys.
        }
        // A non-root id reclaimed in another peer session: this proxy and any
        // retained one name *different* objects. Fall through and mint a new
        // proxy under its own key, leaving retained ones frozen at their own
        // owner sessions so their calls keep being rejected as stale.
      }
    }
    // Capture the session at proxy creation time so we can detect stale
    // proxies that survive a reset().
    const session = this.#sessionId;
    const ownerKey = ownerSession ?? UNKNOWN_SESSION;
    const proxy: object = opaque
      ? // Opaque: simple non-cloneable object (handle)
        {__o: NON_CLONEABLE}
      : // Full proxy with property/method access
        new Proxy(NON_CLONEABLE as object, {
          apply: (_target, _thisArg, args: Array<unknown>) => {
            this.#assertSession(session);
            return this.#makeCall(
              id,
              undefined,
              args,
              this.#remoteProxyOwnerSession.get(proxy),
            );
          },

          get: (_target, prop) => {
            if (prop === INTERNAL) return this;
            // Not thenable at top level (prevents auto-await issues)
            return typeof prop === 'string' && prop !== 'then'
              ? this.#createProxyProperty(id, prop, session, proxy)
              : undefined;
          },

          set: (_target, prop, value) => {
            if (typeof prop !== 'string') return false;
            this.#assertSession(session);
            const transfers: Array<Transferable> = [];
            // Async transport errors cannot be surfaced synchronously
            // from a Proxy set trap. Catch to prevent unhandled rejections.
            void this.#sendCall(
              this.#allocId(),
              id,
              'set',
              prop,
              [this.#toWire(value, '', transfers)],
              transfers,
              this.#remoteProxyOwnerSession.get(proxy),
            ).catch(
              // eslint-disable-next-line @typescript-eslint/no-empty-function
              () => {},
            );
            return true;
          },
        });
    // Freeze the owner session onto this proxy — see the field comments on
    // #remoteProxyById and #remoteProxyOwnerSession for why it is frozen
    // per-proxy rather than re-read by id at call time.
    let entries = byOwner;
    if (entries === undefined) {
      entries = new Map();
      this.#remoteProxyById.set(id, entries);
    }
    entries.set(ownerKey, new WeakRef(proxy));
    this.#remoteProxyOwnerSession.set(proxy, ownerSession);
    this.#remoteProxyByObject.set(proxy, id);
    // The proxy doubles as its own unregister token, so a root revalidation
    // can re-key this registration.
    this.#remoteCleanup.register(proxy, {id, session, ownerKey}, proxy);
    return proxy;
  }

  /**
   * Create a proxy property for lazy property access.
   */
  #createProxyProperty(
    target: number,
    prop: string,
    session: number,
    targetProxy: object,
  ): ProxyProperty {
    // Read the owner session off the proxy at send time rather than
    // capturing it: the root's tag is rewritten in place when the peer
    // re-handshakes (see #createRemoteProxy), and a captured copy would go
    // on tagging with the superseded session.
    const callable = (...args: Array<unknown>): Promise<unknown> => {
      this.#assertSession(session);
      return this.#makeCall(
        target,
        prop,
        args,
        this.#remoteProxyOwnerSession.get(targetProxy),
      );
    };

    callable.then = <TResult1 = unknown, TResult2 = never>(
      onfulfilled?:
        | ((value: unknown) => TResult1 | PromiseLike<TResult1>)
        | null,
      onrejected?:
        | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
        | null,
    ): Promise<TResult1 | TResult2> => {
      this.#assertSession(session);
      return this.#sendCall(
        this.#allocId(),
        target,
        'get',
        prop,
        [],
        [],
        this.#remoteProxyOwnerSession.get(targetProxy),
      ).then(onfulfilled, onrejected);
    };

    (callable as ProxyProperty)[PROXY_PROPERTY_BRAND] = {
      targetProxyId: target,
      property: prop,
    };

    // The callable sends by raw id, so a detached `const fn = remote.method`
    // must keep its proxy alive: otherwise GC collects the proxy, the
    // FinalizationRegistry drops the #remoteProxyById entry, and the owner
    // session goes with it — the call then goes out untagged and skips the
    // peer's staleness check entirely, where a live proxy would have been
    // rejected. Same hazard, and same anchoring, as notify().
    (callable as unknown as Record<symbol, unknown>)[RETAINED_PROXY] =
      targetProxy;

    return callable as ProxyProperty;
  }

  // ============================================================
  // RPC primitives
  // ============================================================

  /**
   * Send a call message and return a promise for the response.
   * The call ID must be pre-allocated by the caller so it can be passed to
   * serialization context (e.g. for handler per-call tracking).
   *
   * `ownerSession` is the frozen tag of the proxy the call was made through
   * (see #remoteProxyOwnerSession); undefined omits the wire field entirely.
   */
  #sendCall(
    callId: number,
    target: number,
    action: 'call' | 'get' | 'set',
    method: string | undefined,
    args: Array<unknown>,
    transfers: Array<Transferable>,
    ownerSession: number | undefined,
  ): Promise<unknown> {
    const {promise, resolve, reject} = Promise.withResolvers<unknown>();
    this.#pendingCalls.set(callId, {resolve, reject});
    try {
      this.#post(
        {
          type: 'call',
          id: callId,
          target,
          action,
          method,
          args,
          ...(ownerSession !== undefined && {session: ownerSession}),
        },
        transfers,
      );
    } catch (error) {
      // postMessage can throw synchronously (e.g. structured-clone failure).
      // Reject the promise instead of propagating synchronously so callers
      // always get a Promise back (maintaining the async contract).
      this.#pendingCalls.delete(callId);
      reject(error instanceof Error ? error : new Error(String(error)));
    }
    return promise;
  }

  #makeCall(
    target: number,
    method: string | undefined,
    args: Array<unknown>,
    ownerSession: number | undefined,
  ): Promise<unknown> {
    const transfers: Array<Transferable> = [];
    // Share seen map across all args to preserve identity for shared references.
    // Allocate the call ID before serialization so it can be threaded through
    // ToWireContext, allowing handlers to track per-call resources.
    const seen = new Map<object, unknown>();
    const callId = this.#allocId();
    return this.#sendCall(
      callId,
      target,
      'call',
      method,
      args.map((arg) =>
        this.#processForClone(arg, '', transfers, seen, callId),
      ),
      transfers,
      ownerSession,
    );
  }

  /**
   * The owner session frozen onto a remote proxy — the tag an outgoing call
   * through `obj` must carry. Read live rather than cached by callers, since
   * the root's tag is rewritten in place when the peer re-handshakes.
   * @internal
   */
  _proxyOwnerSession(obj: object): number | undefined {
    return this.#remoteProxyOwnerSession.get(obj);
  }

  /**
   * Send a one-way call (no response expected, nothing to settle).
   * Failures are caught and logged rather than thrown — the API is void.
   * @internal
   */
  _sendNotify(
    target: number,
    method: string | undefined,
    args: Array<unknown>,
    session: number,
    ownerSession: number | undefined,
  ): void {
    this.#assertSession(session);
    try {
      const transfers: Array<Transferable> = [];
      // Share seen map across all args to preserve identity for shared
      // references. No callId — a notify has nothing to settle.
      const seen = new Map<object, unknown>();
      const wireArgs = args.map((arg) =>
        this.#processForClone(arg, '', transfers, seen),
      );
      this.#post(
        {
          type: 'call',
          id: NOTIFY_ID,
          target,
          action: 'call',
          method,
          args: wireArgs,
          ...(ownerSession !== undefined && {session: ownerSession}),
        },
        transfers,
      );
    } catch (error) {
      this.#logger?.error?.('Failed to send notify', error);
    }
  }

  // ============================================================
  // Message handling
  // ============================================================

  #onMessage = (event: MessageEvent<Message | BatchMessage>): void => {
    const data = event.data;
    // Some MessagePort implementations can deliver null/undefined data
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (data == null) return;

    // Unconditionally handle batch messages from any sender
    if (data.type === 'batch') {
      for (const message of data.messages) {
        this.#processMessage(message);
      }
    } else {
      this.#processMessage(data);
    }
  };

  #processMessage(message: Message): void {
    switch (message.type) {
      case 'release': {
        // A release tagged with a session we've since reset() past names an
        // id that now belongs to an unrelated object — honoring it would
        // unregister a live target. An absent tag comes from a peer that
        // predates the field and releases unconditionally, as before.
        if (
          message.session !== undefined &&
          message.session !== this.#sessionId
        ) {
          this.#logger?.debug?.(
            'Ignoring release for a superseded session (peer reset)',
            message.id,
          );
          break;
        }
        // Unified release for both proxies and handles
        const obj = this.#localById.get(message.id);
        if (obj !== undefined) {
          this.#localById.delete(message.id);
          this.#localByObject.delete(obj);
        }
        break;
      }
      case 'resolve':
        this.#settlePending(
          this.#pendingRemotePromises,
          message.id,
          message.value,
        );
        break;
      case 'reject':
        this.#rejectPending(
          this.#pendingRemotePromises,
          message.id,
          message.error,
        );
        break;
      case 'return':
        if (message.id === HANDSHAKE_ID && message.session !== undefined) {
          // Record the peer's session from every handshake, not just the
          // first — a peer that reset() and re-exposed sends a new one, and
          // we need the latest value regardless of whether we still have a
          // pending waitForReady() call to settle (we may not: only the
          // side that calls waitForReady() again after its own reset does).
          // Guarded on the field being present: an ordinary return that
          // happens to use id 0 must not clear a known peer session, and an
          // older peer sends none at all.
          // Must happen before #settlePending below, which deserializes
          // `message.value` into the root proxy: a peer that predates the `s`
          // wire field has no owner session on that proxy, so the root falls
          // back to this value and would otherwise be tagged with the
          // previous session and rejected as stale on every call.
          this.#peerSession = message.session;
          if (
            !this.#pendingCalls.has(HANDSHAKE_ID) &&
            this.#root !== undefined
          ) {
            // Nothing is awaiting readiness, so #settlePending below won't
            // deserialize the value — but a consumer that never calls
            // waitForReady() a second time is still holding the root from
            // the previous handshake. A root names "the peer's root
            // service", a stable role rather than one particular object, so
            // revalidate the retained root against the re-exposed one:
            // deserializing runs #createRemoteProxy, which re-keys it to the
            // new owner session, and the discarded result fires nothing
            // else. Guarded so a pending handshake doesn't deserialize the
            // value twice, and on a root actually being retained — after
            // our own reset()/close() there is nothing to revalidate, and
            // deserializing would mint a throwaway proxy whose eventual GC
            // posts a release against the peer's live root.
            this.#fromWire(message.value);
          }
        }
        this.#settlePending(this.#pendingCalls, message.id, message.value);
        this.#notifyCallSettle(message.id);
        break;
      case 'throw':
        this.#rejectPending(this.#pendingCalls, message.id, message.error);
        this.#notifyCallSettle(message.id);
        break;
      case 'call':
        void this.#handleCall(message);
        break;
      case 'handler':
        this.#handleHandlerMessage(message.wireType, message.payload);
        break;
      default:
        // Exhaustiveness check
        message satisfies never;
    }
  }

  #settlePending(
    map: Map<number, PendingCall>,
    id: number,
    value: WireValue,
  ): void {
    const pending = map.get(id);
    if (pending) {
      map.delete(id);
      try {
        pending.resolve(this.#fromWire(value));
      } catch (error) {
        pending.reject(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    }
  }

  #rejectPending(
    map: Map<number, PendingCall>,
    id: number,
    error: SerializedError,
  ): void {
    const pending = map.get(id);
    if (pending) {
      map.delete(id);
      pending.reject(deserializeError(error));
    }
  }

  #notifyCallSettle(callId: number): void {
    for (const handler of this.#handlers) {
      handler.onCallSettle?.(callId);
    }
  }

  /**
   * Route a handler message to the appropriate handler.
   */
  #handleHandlerMessage(wireType: string, payload: WireValue): void {
    try {
      const handler = this.#handlersByWireType.get(wireType);
      if (handler?.onMessage) {
        const seen = new Map<object, unknown>();
        handler.onMessage(
          this.#processFromClone(payload, seen),
          this.#makeFromWireContext(seen),
        );
      }
    } catch (error) {
      // Log errors from onMessage but don't propagate them
      // (there's no good place to send them - these are spontaneous messages)
      this.#logger?.error?.(
        `Error in handler.onMessage for wireType "${wireType}":`,
        error,
      );
    }
  }

  async #handleCall(message: CallMessage): Promise<void> {
    const {id, target, method, args, action} = message;
    const oneWay = id === NOTIFY_ID;

    // Deserialize arguments with shared seen map to preserve identity
    const seen = new Map<object, unknown>();
    const deserializedArgs = args.map((arg) =>
      this.#processFromClone(arg, seen),
    );

    // Reject calls stamped with a session we've since reset() past. This is
    // the case the missing-target check below cannot catch: after a reset(),
    // ids restart from 0, so a stale call can name an id that is not
    // missing — it now belongs to an unrelated object registered in the new
    // session. `session` is only present once the sender has seen at least
    // one of our handshakes and is absent from older peers, so an absent
    // value skips this check entirely rather than counting as a mismatch.
    //
    // Deliberately after deserialization, like the missing-target check:
    // dropping the args on the wire would strand every resource they carry —
    // proxy args pinned in the sender's registry with no proxy here to ever
    // release them, transferables never adopted, handler-serialized args
    // (e.g. a signal subscription) left streaming forever. Deserializing is
    // safe: the arg proxies name objects the *sender* registered in its own
    // current session, and the result is simply discarded.
    if (message.session !== undefined && message.session !== this.#sessionId) {
      const error = {
        name: 'ReferenceError',
        message: 'Stale session',
      };
      if (oneWay) {
        this.#logger?.debug?.(
          'Dropping one-way call for a superseded session (peer reset)',
          error,
        );
        return;
      }
      this.#post({type: 'throw', id, error});
      return;
    }

    // Look up the target object
    const proxyTarget = this.#getLocal(target);
    if (!proxyTarget) {
      const error = {
        name: 'ReferenceError',
        message: `Proxy target ${String(target)} not found`,
      };
      if (oneWay) {
        // Expected protocol debris, not a failure: a one-way call has no
        // reply channel. A reused id that now maps to a different object is
        // already caught by the session check above, so a genuinely missing
        // target here just means in-flight debris — e.g. a release message
        // and a call crossing paths, or a call for an id the peer never
        // finished registering. Log at debug so a real leak is still
        // diagnosable without paging every reset.
        this.#logger?.debug?.(
          'Dropping one-way call for unknown proxy target (peer likely reset)',
          error,
        );
        return;
      }
      return this.#post({type: 'throw', id, error});
    }

    const logger = this.#logger;
    const start = logger?.debug ? performance.now() : 0;

    try {
      let result: unknown;

      if (action === 'get') {
        if (method === undefined) {
          throw new TypeError('Property name required for get action');
        }
        result = (proxyTarget as Record<string, unknown>)[method];
      } else if (action === 'set') {
        if (method === undefined) {
          throw new TypeError('Property name required for set action');
        }
        (proxyTarget as Record<string, unknown>)[method] = deserializedArgs[0];
        result = undefined;
      } else if (method === undefined) {
        // Direct function invocation
        if (typeof proxyTarget !== 'function') {
          throw new TypeError('Target is not callable');
        }
        result = await (proxyTarget as (...a: Array<unknown>) => unknown)(
          ...deserializedArgs,
        );
      } else {
        // Method invocation
        const targetObj = proxyTarget as Record<string, unknown>;
        const value = targetObj[method];
        if (typeof value !== 'function') {
          throw new TypeError(`${method} is not a function`);
        }
        result = await (value as (...a: Array<unknown>) => unknown).apply(
          proxyTarget,
          deserializedArgs,
        );
      }

      if (!oneWay) {
        const transfers: Array<Transferable> = [];
        const wire = this.#toWire(result, '', transfers);
        this.#post({type: 'return', id, value: wire}, transfers);
      }
      logger?.debug?.(`${action} ${method ?? '(direct)'} completed`, {
        duration: performance.now() - start,
      });
    } catch (error) {
      logger?.debug?.(`${action} ${method ?? '(direct)'} failed`, {
        duration: performance.now() - start,
        error,
      });
      if (oneWay) {
        logger?.error?.('Uncaught error in notify handler', error);
        return;
      }
      this.#post({type: 'throw', id, error: serializeError(error)});
    }
  }
}
