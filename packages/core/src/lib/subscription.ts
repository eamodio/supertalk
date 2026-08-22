/**
 * Synchronous subscription handles.
 *
 * `subscribe()` returns a `Subscription` immediately and buffers the wire
 * subscribe internally until the connection's handshake completes, so a
 * consumer can subscribe from a constructor without awaiting anything. The
 * subscriber re-runs on every subsequent handshake (i.e. after a
 * `reset()` + `waitForReady()` reconnect), so the library — not the
 * application — owns resubscription.
 *
 * @fileoverview Client-side API for subscribe().
 */

import type {Remote} from './types.js';
import {Connection} from './connection.js';
import {ConnectionClosedError, connectionOf, isPromise} from './protocol.js';

/**
 * An unsubscribe function returned by a subscriber. Its return value (if
 * any) is ignored by the caller — call it to release the remote-side
 * subscription.
 */
export type Unsubscribe = () => unknown;

/**
 * A handle returned by `subscribe()`.
 */
export interface Subscription extends Disposable {
  /**
   * Stop receiving: releases the currently held remote subscription (if
   * any) and cancels resubscription on future reconnects. Idempotent.
   */
  unsubscribe(): void;
  /** True once `unsubscribe()` has been called. */
  readonly closed: boolean;
  /**
   * Resolves when the initial subscribe call lands; rejects if it fails.
   * Lazy: reading this getter is what creates the promise, so an ignored
   * failure never becomes an unhandled rejection — but failures are always
   * reported through the connection's `logger`, when a connection was
   * resolved, regardless of whether anything reads `ready`. A rejected
   * `target` promise (or an invalid target) has no connection to log
   * through, so that failure surfaces only on `ready`.
   */
  readonly ready: Promise<void>;
}

// `void` in this union is part of the public API shape (a subscriber may
// return nothing), not a mistake `no-invalid-void-type` would otherwise
// catch.
/* eslint-disable @typescript-eslint/no-invalid-void-type */
type Subscriber<T> = (
  remote: Remote<T>,
) => Unsubscribe | void | Promise<Unsubscribe | void>;
/* eslint-enable @typescript-eslint/no-invalid-void-type */

/**
 * Subscribe to a remote service without waiting for the connection.
 *
 * Returns a `Subscription` synchronously and buffers the wire subscribe
 * call until the connection's handshake completes — so you can call this
 * from a constructor and never `await` anything. When passing a bare
 * `Connection`, the application must still drive `waitForReady()` (or use
 * `wrap()`), since that call is what signals ready and triggers delivery.
 *
 * The `subscriber` thunk receives the current root proxy and does whatever
 * subscribing means for that service (`onFoo(cb)`, `onFoo(filter, cb)`,
 * `watch({...})`); its return value, if a function, is treated as the
 * remote-side unsubscribe. It may run more than once — once per successful
 * handshake, including every reconnect — so it must be idempotent. Only
 * subscriptions anchored on the **root** proxy resurrect across a
 * reconnect: a proxy from a dead session is gone by definition, so the
 * subscriber must always re-derive whatever it subscribes on from the
 * `remote` argument it is given.
 *
 * Errors from the subscriber (a throw, a rejection) are reported through
 * the connection's `logger` option and surface on `ready`.
 * `ConnectionClosedError` is never logged, since it means deliberate
 * teardown: a `reset()` is swallowed entirely (the next `ready` re-issues
 * automatically), while a `close()` settles `ready` with the error so
 * awaiting it cannot hang.
 *
 * @param target - Used only to locate the connection: a `Connection`, the
 * root proxy from `wrap()`, or a `Promise` of one (e.g. `wrap()`'s return
 * value). A nested (non-root) proxy is not supported — the subscriber is
 * always called with the current root proxy for the session, so the
 * `remote` it receives would not match a nested `target`'s type.
 * @param subscriber - Called with the current root proxy on every
 * successful handshake.
 */
export function subscribe<T extends object>(
  target: Connection | Remote<T> | Promise<Remote<T>>,
  subscriber: Subscriber<T>,
): Subscription {
  return new SubscriptionImpl(target, subscriber);
}

/**
 * Resolve the `Connection` owning a subscribe target. Accepts a `Connection`
 * directly, or reads the `INTERNAL` symbol off a remote proxy — the same
 * hook `notify()` uses. This only locates the connection; it does not
 * establish which proxy the subscriber runs against — that is always the
 * connection's current root, per `subscribe()`'s contract.
 */
function resolveConnection(value: unknown): Connection {
  if (value instanceof Connection) return value;
  const connection = connectionOf(value);
  if (connection === undefined) {
    throw new TypeError('subscribe() requires a Connection or a remote proxy');
  }
  // The subscriber is always called with the current ROOT proxy, so a
  // nested target would silently receive the wrong object — reject anything
  // that has never been a handshake root. Root-ness survives reset(), so a
  // retained pre-reset root still works as a connection locator while a
  // stale nested proxy is still rejected.
  if (!connection._isRootProxy(value as object)) {
    throw new TypeError(
      'subscribe() requires the root proxy — the subscriber always receives ' +
        'the root, so a nested proxy target is not supported',
    );
  }
  return connection;
}

class SubscriptionImpl<T extends object> implements Subscription {
  #connection: Connection | undefined;
  #closed = false;
  #offReady: (() => void) | undefined;
  #offClosed: (() => void) | undefined;
  #generation = 0;
  #currentUnsubscribe: Unsubscribe | undefined;
  // The session the stored unsubscribe was issued in — a reset() bumps the
  // connection's session, at which point the peer's subscription state is
  // already gone and invoking the stale unsubscribe could only throw.
  #currentUnsubscribeSession: number | undefined;

  #readySettled = false;
  #readyFailed = false;
  #readyError: unknown;
  #readyDeferred: PromiseWithResolvers<void> | undefined;

  constructor(
    target: Connection | Remote<T> | Promise<Remote<T>>,
    subscriber: Subscriber<T>,
  ) {
    if (target instanceof Connection) {
      this.#attach(target, subscriber);
      return;
    }
    if (isPromise(target)) {
      target.then(
        (remote) => {
          if (this.#closed) return;
          try {
            this.#attach(resolveConnection(remote), subscriber);
          } catch (error) {
            this.#fail(error);
          }
        },
        (error: unknown) => {
          this.#fail(error);
        },
      );
      return;
    }
    try {
      this.#attach(resolveConnection(target), subscriber);
    } catch (error) {
      this.#fail(error);
    }
  }

  get closed(): boolean {
    return this.#closed;
  }

  get ready(): Promise<void> {
    // Memoized: every read shares one promise, created lazily on first
    // read so an ignored failure never becomes an unhandled rejection.
    if (this.#readyDeferred === undefined) {
      this.#readyDeferred = Promise.withResolvers();
      if (this.#readySettled) {
        if (this.#readyFailed) {
          this.#readyDeferred.reject(this.#readyError as Error);
        } else {
          this.#readyDeferred.resolve();
        }
      }
    }
    return this.#readyDeferred.promise;
  }

  unsubscribe(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#offReady?.();
    this.#offReady = undefined;
    this.#offClosed?.();
    this.#offClosed = undefined;
    const unsub = this.#currentUnsubscribe;
    this.#currentUnsubscribe = undefined;
    // Skip the remote release if a reset() has moved the connection past
    // the session the unsubscribe belongs to — the peer's state is gone,
    // and the stale proxy would just throw.
    if (
      unsub !== undefined &&
      this.#connection?._session === this.#currentUnsubscribeSession
    ) {
      this.#invokeRemoteUnsubscribe(unsub);
    }
    // Unsubscribing settles `ready` (first time only) so awaiting it can
    // never hang, even when called before the initial subscribe landed.
    this.#settleReadySuccess();
  }

  [Symbol.dispose](): void {
    this.unsubscribe();
  }

  #attach(connection: Connection, subscriber: Subscriber<T>): void {
    if (this.#closed) return;
    this.#connection = connection;
    this.#offReady = connection._onReady((root) => {
      this.#issue(root as Remote<T>, subscriber);
    });
    this.#offClosed = connection._onClosed(() => {
      // close() settles a still-pending ready so awaiting it can never hang.
      // The subscription itself stays registered: if the connection is later
      // reset() and reconnected, the next ready re-issues as usual.
      this.#settleReadyFailure(new ConnectionClosedError('closed'));
    });
  }

  #issue(root: Remote<T>, subscriber: Subscriber<T>): void {
    if (this.#closed) return;
    // Drop the previous session's unsubscribe without calling it — the
    // peer's state from that session is already gone.
    this.#currentUnsubscribe = undefined;
    const generation = ++this.#generation;
    // Normalize a synchronous throw into a rejection.
    void (async () => subscriber(root))().then(
      (result) => {
        this.#onIssueSuccess(generation, result);
      },
      (error: unknown) => {
        this.#onIssueFailure(generation, error);
      },
    );
  }

  // `result` is the subscriber's Awaited return value, which may be `void`.
  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
  #onIssueSuccess(generation: number, result: Unsubscribe | void): void {
    // A newer issue (a later reconnect) has already superseded this one.
    if (generation !== this.#generation) return;
    const unsub = typeof result === 'function' ? result : undefined;
    if (this.#closed) {
      // unsubscribe() ran while this issue was in flight — release the
      // remote side immediately instead of storing the handle.
      if (unsub !== undefined) this.#invokeRemoteUnsubscribe(unsub);
      return;
    }
    this.#currentUnsubscribe = unsub;
    this.#currentUnsubscribeSession = this.#connection?._session;
    this.#settleReadySuccess();
  }

  #onIssueFailure(generation: number, error: unknown): void {
    if (generation !== this.#generation) return;
    if (this.#closed) return;
    if (error instanceof ConnectionClosedError) {
      if (error.reason === 'reset') {
        // Deliberate reconnect — the next ready re-issues.
        return;
      }
      // 'closed': deliberate teardown — settle ready without logging so
      // awaiting it can't hang, but don't report it as a failure.
      this.#settleReadyFailure(error);
      return;
    }
    this.#connection?._logError('Subscription failed', error);
    this.#settleReadyFailure(error);
  }

  #invokeRemoteUnsubscribe(unsub: Unsubscribe): void {
    try {
      const result = unsub();
      if (isPromise(result)) {
        result.catch((error: unknown) => {
          if (error instanceof ConnectionClosedError) return;
          this.#connection?._logError('Subscription unsubscribe failed', error);
        });
      }
    } catch (error) {
      if (error instanceof ConnectionClosedError) return;
      this.#connection?._logError('Subscription unsubscribe failed', error);
    }
  }

  #fail(error: unknown): void {
    if (this.#closed) return;
    this.#connection?._logError('Subscription failed', error);
    this.#settleReadyFailure(error);
  }

  #settleReadySuccess(): void {
    if (this.#readySettled) return;
    this.#readySettled = true;
    this.#readyDeferred?.resolve();
  }

  #settleReadyFailure(error: unknown): void {
    if (this.#readySettled) return;
    this.#readySettled = true;
    this.#readyFailed = true;
    this.#readyError = error;
    this.#readyDeferred?.reject(error);
  }
}
