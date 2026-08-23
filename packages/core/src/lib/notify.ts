/**
 * One-way, fire-and-forget calls against a remote proxy.
 *
 * A notify sends a single `CallMessage` stamped with the reserved
 * `NOTIFY_ID` sentinel and never registers a pending call, so there is no
 * response to await and nothing to leak as an unhandled rejection.
 *
 * @fileoverview Client-side API for one-way notify calls.
 */

import {NON_CLONEABLE} from './constants.js';
import {connectionOf} from './protocol.js';

/**
 * Transforms a remote proxy type into a one-way notifier: methods (or a
 * callable target) return `void` instead of `Promise<...>`. Non-method
 * properties have no notify equivalent and are typed `never`.
 */
export type Notify<T> = T extends (...args: infer A) => unknown
  ? (...args: A) => void
  : {
      [K in keyof T]: T[K] extends (...args: infer A) => unknown
        ? (...args: A) => void
        : never;
    };

/**
 * Create a one-way notifier over a remote proxy.
 *
 * Calling the notifier (or one of its methods) sends exactly one wire
 * message and does not wait for, or expect, a response — the receiver skips
 * posting a `return`/`throw` back. There is no promise to await or reject:
 * errors on either side (serialization failures on the sender, a throwing
 * handler on the receiver) are reported through the connection's `logger`
 * option instead.
 *
 * `notify()` accepts any remote proxy — the root from `wrap()`, a nested
 * proxy, or a function proxy received as an argument (the callback/event
 * case) — and throws `TypeError` on anything else, including a `handle()`,
 * which has no callable surface.
 *
 * The returned notifier is a plain object; hoist it out of hot loops the
 * same way you would hoist `remote.method`, rather than calling `notify()`
 * on every invocation.
 *
 * Do not pass per-call resources (e.g. `AbortSignal`) through `notify()`: a
 * handler that releases its resource on call settlement (`onCallSettle`)
 * will never be told to release, since a notify never settles.
 *
 * A notifier itself is not transferable over the wire — it is a local
 * wrapper around a specific proxy id on this connection, not a proxy that
 * can be sent to the other side.
 *
 * The notifier retains `target` for its own lifetime, so holding only the
 * notifier is enough to keep the remote registration alive — dropping the
 * proxy everywhere else does not trigger the GC `release` handshake that
 * would tell the peer to forget the object.
 *
 * After a `reset()`, calling a notifier created in the previous session
 * throws `'Stale proxy from previous session'`, the same as calling a stale
 * proxy.
 *
 * @param target - A remote proxy (or a function proxy) to notify through.
 * @returns A notifier with the same shape as `target`, but void-returning.
 */
export function notify<T>(target: T): Notify<T> {
  const connection = connectionOf(target);
  if (connection === undefined) {
    throw new TypeError('notify() requires a remote proxy');
  }

  const id = connection._proxyId(target as object);
  if (id === undefined) {
    // It carries the INTERNAL hook, so it IS a remote proxy — just one from
    // a session that a reset() has since discarded.
    throw new Error('Stale proxy from previous session');
  }
  const session = connection._session;

  // Cache per-property notifier functions so hot loops that read the same
  // method off the notifier repeatedly don't re-allocate.
  const cache = new Map<string, (...args: Array<unknown>) => void>();

  // The notifier must keep `target` alive for its own lifetime. It sends by
  // raw id, so if the caller drops every other reference to the proxy (the
  // typical event-callback pattern: `handlers.set(key, notify(cb))`), GC
  // would collect it, the FinalizationRegistry would post `release` for the
  // id, and the peer would drop the object — every later notify then lands
  // on an unknown target and is silently discarded. Anchoring the proxy on
  // the handler object ties its lifetime to the notifier's.
  const handler: ProxyHandler<object> & {retained?: unknown} = {
    apply: (_target, _thisArg, args: Array<unknown>) => {
      connection._sendNotify(id, undefined, args, session);
    },

    get: (_target, prop) => {
      if (typeof prop !== 'string' || prop === 'then') return undefined;
      let fn = cache.get(prop);
      if (fn === undefined) {
        fn = (...args: Array<unknown>) =>
          connection._sendNotify(id, prop, args, session);
        cache.set(prop, fn);
      }
      return fn;
    },
  };
  handler.retained = target;

  return new Proxy(NON_CLONEABLE as object, handler) as Notify<T>;
}
