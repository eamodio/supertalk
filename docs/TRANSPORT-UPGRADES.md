# Transport Upgrades

Design and implementation plan for three transport primitives:

| Item    | Feature                                                        | Milestone |
| ------- | -------------------------------------------------------------- | --------- |
| **P1a** | One-way `notify()` calls (no response frame)                   | M1        |
| **P1b** | Synchronous `subscribe()` handles + library-owned resubscribe  | M2        |
| **P2b** | Deliberate-teardown call settlement (folded into M2)           | M2        |
| **P1c** | `SequencedChannel` — ordered delivery, gap events, generations | M3        |

Driver: a consumer (GitLens) migrated a large webview from a hand-rolled `postMessage`
protocol to Supertalk and hit the same three edges repeatedly. Each item below states the
consumer boilerplate it deletes.

Non-goals: no serialization-format change, no changes to `@eamodio/supertalk-signals`
beyond what P1b touches incidentally.

---

## Design constraints

**Wire compatibility.** The library is pre-release and both ends of a connection normally
ship in the same bundle, but a consumer can pin different versions per surface. Every wire
change below is therefore designed so that a **new sender talking to an old receiver keeps
working** — no silent message loss. Where that is impossible the feature is opt-in and the
plan says so.

**Size.** The `checksize` bundle (`packages/core/index.js`, everything the barrel exports)
is the budget. Two rules:

1. Anything that can live outside `Connection` does, in its own module, so consumers who
   do not import it tree-shake it away.
2. Hooks inside `Connection` are kept to single branches, not subsystems.

The checksize report bundles the whole barrel, so it always shows the sum. M1 adds a second
rollup input (`checksize/minimal.js`, importing only `expose`/`wrap`) so the report shows
both the total and what a consumer who ignores the new APIs actually pays.

**Both runtimes.** Every feature gets `node:test` coverage under `src/test/node/` and a
`@web/test-runner` smoke test under `src/test/browser/`. Browser tests use `MessageChannel`
with explicit `port.start()`, matching `basic_test.ts`.

---

## P1a — One-way notify

### Problem

Every call is request/response. An event emission or a fire-and-forget write costs two wire
messages, and the caller holds a promise nobody awaits. For a consumer whose hottest path is
selection reporting plus ~20 event channels, the ack is pure overhead — and an unawaited
promise turns into an unhandled rejection when the connection goes away mid-flight.

### Public API

```ts
import {notify} from '@eamodio/supertalk';

// Method calls on a remote object
notify(remote).reportSelection(ids); // void — one wire message, no ack

// A callback proxy received from the other side (the event-emission case)
class Service {
  onDidChange(cb: (e: Change) => void) {
    const emit = notify(cb); // hoist once
    this.#listeners.add(emit);
    return () => this.#listeners.delete(emit);
  }
}
```

```ts
export type Notify<T> = T extends (...args: infer A) => unknown
  ? (...args: A) => void
  : {
      [K in keyof T]: T[K] extends (...args: infer A) => unknown
        ? (...args: A) => void
        : never;
    };

export function notify<T>(target: T): Notify<T>;
```

`notify()` accepts any remote proxy — the root from `wrap()`, a nested proxy, or a function
proxy received as an argument. It throws `TypeError` on a non-proxy (including a `handle()`,
which has no callable surface) so the mistake surfaces at the call site rather than as a
silently dropped message. The returned notifier is a plain JS `Proxy`; hoist it out of hot
loops the same way you would hoist `remote.method`.

### Wire protocol

**No new message type.** A one-way call is a `CallMessage` whose `id` is the reserved
sentinel `NOTIFY_ID = -1` (real IDs are non-negative; `0` is the handshake):

```jsonc
{"type": "call", "id": -1, "target": 3, "action": "call", "method": "reportSelection", "args": [...]}
```

The receiver checks `id === NOTIFY_ID` in `#handleCall` and skips the `return`/`throw` post.
The sender never registers a pending call.

Compatibility:

| Sender | Receiver | Behavior                                                                                                                                                                                                |
| ------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| new    | new      | One message. No response.                                                                                                                                                                               |
| new    | old      | Call is **executed correctly**; the old receiver posts `return`/`throw` with `id: -1`, which the new sender drops (no pending call matches). One wasted ack — degrades to today's cost, nothing breaks. |
| old    | new      | N/A — old senders never emit `-1`.                                                                                                                                                                      |

**Alternative considered and rejected:** a distinct `{type: 'notify'}` message. It reads
better on the wire, but an old receiver's `#processMessage` switch has no arm for it, so the
call would be **silently dropped** — exactly the silent protocol break the constraint
forbids. The sentinel ID costs one comparison and degrades safely.

### Errors

There is no wire response, so errors surface locally on whichever side produced them, via the
existing `logger` option (no new API):

- **Sender side** — serialization or `postMessage` throwing is caught and reported through
  `logger.error`. It is not rethrown: a one-way send is void and must not break the caller's
  synchronous flow.
- **Receiver side** — a throwing target is caught in `#handleCall` and reported through that
  side's `logger.error` instead of being posted back.

### Composition

- **Batching** — notifies go through `#post` like everything else, so they coalesce into
  `{type: 'batch'}` and keep ordering relative to regular calls.
- **Handlers** — arguments serialize through the normal `#processForClone` path, so
  `proxy()`, `transfer()`, and custom handlers all work. `ToWireContext.callId` is
  `undefined` for a notify, since there is no call to settle: **do not pass per-call
  resources (e.g. `AbortSignal`) through `notify()`** — a handler that releases on
  `onCallSettle` would never be told to release. Documented in the JSDoc.
- **`reset()` / `close()`** — a notify sent on a closed connection is dropped by `#post`,
  same as any other message, and produces no rejection (that is the point).

### Size impact

Estimate ~150–250 B brotli in the barrel: one exported function, the `Notify<T>` type (free),
a symbol branch in the remote-proxy `get` trap, and one `if` in `#handleCall`. The notifier
factory lives in `lib/notify.ts` and is reachable only from `notify()`, so consumers who
never import it shed it.

### Tests

Node (`notify_test.ts`):

1. A notify produces exactly one `postMessage` and no response (spy endpoint, as in
   `batching_test.ts`).
2. The remote method actually runs with the right arguments.
3. `notify()` on a function proxy (the callback/event case) invokes the callback.
4. A throwing remote method logs on the receiver and does not post a `throw`, and does not
   reject anything on the sender.
5. A serialization failure on the sender logs and does not throw out of the call.
6. Notifies batch with regular calls and preserve ordering.
7. `notify()` on a non-proxy throws `TypeError`.
8. Interop: a notify against a receiver that answers everything (simulated old peer) does not
   produce a stray rejection or a stuck pending call.

Browser (`notify_test.ts`): one-message-no-ack over a `MessageChannel`, plus the callback case.

### Consumer migration note

```diff
- void remote.reportSelection(ids).catch(handleClosedConnection);
+ this.#notifier ??= notify(remote);
+ this.#notifier.reportSelection(ids);
```

For event fan-out, the emitting side hoists one notifier per subscriber callback and calls it
directly; the `.catch()` wrappers that existed only to swallow teardown rejections go away
because there is no promise.

---

## P1b — Synchronous subscription handles

### Problem

Subscribing is `await remote.onEvent(cb)` returning `Promise<Unsubscribe>`. Consequences:
subscribe races teardown and reconnect, so every call site carries a staleness guard plus an
unsubscribe-if-superseded dance; the returned unsubscribe needs casts; and after a reconnect
the application must re-subscribe everything by hand.

### Public API

```ts
import {subscribe} from '@eamodio/supertalk';

class Panel {
  // In a constructor. Nothing awaited. Survives reconnect.
  #rows = subscribe(this.#connection, (remote) =>
    remote.onDidChangeRows(this.#onRows),
  );

  dispose() {
    this.#rows.unsubscribe();
  }
}
```

```ts
export type Unsubscribe = () => unknown;

export interface Subscription extends Disposable {
  /** Stop receiving; releases the remote subscription and cancels resubscription. */
  unsubscribe(): void;
  /** True once `unsubscribe()` has been called. */
  readonly closed: boolean;
  /**
   * Resolves when the initial subscribe call lands (rejects if it fails).
   * Lazy: errors are logged whether or not anything reads this.
   */
  readonly ready: Promise<void>;
}

export function subscribe<T extends object>(
  target: Connection | Remote<T> | Promise<Remote<T>>,
  subscriber: (
    remote: Remote<T>,
  ) => Unsubscribe | void | Promise<Unsubscribe | void>,
): Subscription;
```

The **subscriber thunk** — not a method name — is what makes this general: it receives the
current root proxy and does whatever subscribing means for that service (`onFoo(cb)`,
`onFoo(filter, cb)`, `watch({...})`). Its return value, if any, is the unsubscribe.

Accepting a `Connection` is the primary form for consumers that own reconnect; the
`Remote`/`Promise<Remote>` forms exist so `wrap()` users get the same ergonomics
(`subscribe(wrap<T>(ep), ...)` — note there is no `await`).

Behavior:

- **Synchronous return.** The handle exists immediately. If the connection has not finished
  its handshake, the subscribe call is buffered internally and issued on ready.
- **`unsubscribe()` before the call lands** cancels it: when the in-flight subscribe resolves,
  the library immediately invokes the returned unsubscribe and drops it. Idempotent.
- **Reconnect.** The library re-invokes the subscriber with the new root on every subsequent
  handshake. The old session's unsubscribe is discarded, not called — the peer's state is gone.
- **Errors.** A failed subscribe is reported through `logger.error`. `ConnectionClosedError`
  (below) is swallowed rather than logged, since it means deliberate teardown.

Documented limits: only subscriptions anchored on the **root** proxy resurrect (proxies from a
dead session are gone by definition), and the subscriber must be idempotent because it can run
more than once.

### Wire protocol

**None.** P1b is client-side machinery over the existing `call` path. That is the whole
appeal: zero compatibility surface, works against any peer version.

### Connection changes

`Connection` gains a `#root`, a ready-callback set, and one internal hook:

```ts
/** @internal — invoke `cb` with the root proxy on every successful handshake. */
_onReady(cb: (root: object) => void): () => void;
```

`waitForReady()` chains onto its own handshake promise to record `#root` and fan out to the
callbacks; if a subscription registers after ready, its callback fires immediately. All
subscription bookkeeping lives in `lib/subscription.ts`, so the cost inside `Connection` is
the field, the set, and the hook.

`subscribe()` reaches the connection from a `Remote` through the same internal symbol
`notify()` uses (`get` trap on the remote proxy returns the owning `Connection`).

### P2b — teardown settlement (folded in here)

Reconnect and teardown are the same code path, so this lands with M2:

```ts
export class ConnectionClosedError extends Error {
  readonly reason: 'closed' | 'reset';
}
```

`close()` and `reset()` settle in-flight calls and promises with it instead of a bare
`new Error('Connection closed')`, so consumers can filter deliberate teardown from real
failures. `Connection` also gains `[Symbol.dispose]()` (= `close()`) for `using`.

**What is deliberately not done:** a "swallow" disposal option. Supertalk hands the caller a
promise it does not own; the only ways to stop an unhandled rejection are to never settle it
(a hang) or to resolve it with a lie. The honest answers are (a) filter on the error type, or
(b) use `notify()` and hold no promise at all. Documented as such.

### Composition

- **Batching / handlers** — the subscribe call is an ordinary call.
- **`reset()`** — `#dropPendingWork` rejects the in-flight subscribe with
  `ConnectionClosedError`; the subscription treats that as "wait for the next ready" rather
  than a failure.
- **Signals** — untouched. `SignalHandler` does its own watch/unwatch over handler messages
  and already cycles on `disconnect()`/`connect()`.

### Size impact

Estimate ~250–400 B brotli: `lib/subscription.ts` plus the `_onReady` hook and
`ConnectionClosedError`. Only the hook and the error class are unconditional.

### Tests

Node (`subscription_test.ts`):

1. Subscribe before ready (against a not-yet-exposed peer) delivers events once ready.
2. `unsubscribe()` stops delivery and calls the remote unsubscribe.
3. `unsubscribe()` called before the subscribe lands still releases the remote side (assert
   the service's unsubscribe ran).
4. Reconnect (`reset()` + re-expose + `waitForReady()`) re-runs the subscriber and delivery
   resumes with no application-side machinery.
5. A subscriber that throws / rejects logs once and leaves the handle usable.
6. `ready` resolves on success and rejects on failure; no unhandled rejection when ignored.
7. `closed` transitions; `unsubscribe()` is idempotent; `Symbol.dispose` works with `using`.
8. `subscribe(promiseOfRemote, ...)` and `subscribe(remote, ...)` forms.
9. `close()`/`reset()` reject in-flight calls with `ConnectionClosedError` carrying the right
   `reason`, and `instanceof Error` still holds.

Browser (`subscription_test.ts`): subscribe-then-receive and unsubscribe over a
`MessageChannel`.

### Consumer migration note

```diff
- let unsub: (() => void) | undefined;
- let generation = ++this.#connectGeneration;
- void remote.onDidChangeRows(this.#onRows).then(u => {
-   if (generation !== this.#connectGeneration) { void (u as unknown as () => void)(); return; }
-   unsub = u as unknown as () => void;
- });
+ this.#rows = subscribe(this.#connection, r => r.onDidChangeRows(this.#onRows));
```

and the app's `reconnect()` no longer re-subscribes anything.

---

## P1c — Sequenced channel

### Problem

A delta stream (row splices against a ledger) needs strictly ordered delivery, receiver-side
gap detection, and a generation/epoch the sender can bump to invalidate stale in-flight
messages. Consumers hand-roll this per stream. The library should own ordering and gap
detection; the **domain** recovery (what to resend) stays with the application.

### Public API

A handler, in its own module — `@eamodio/supertalk-core/handlers/channel.js` — so it costs
nothing unless imported.

```ts
import {SequencedChannel} from '@eamodio/supertalk-core/handlers/channel.js';

// Sender
const rows = new SequencedChannel<RowSplice>('rows', {replay: 32});
expose(service, endpoint, {handlers: [rows]});
rows.send(splice); // stamped {generation, seq}
rows.newGeneration(); // epoch bump: resets seq, clears replay, invalidates in-flight

// Receiver
const rows = new SequencedChannel<RowSplice>('rows');
const connection = new Connection(endpoint, {handlers: [rows]});
rows.subscribe((splice, meta) => ledger.apply(splice)); // in-order only
rows.onGap((gap) => void service.resyncRows()); // domain recovery is the app's
```

```ts
export interface SequencedChannelOptions {
  /** Keep the last N sent messages so a receiver gap can be repaired automatically. */
  replay?: number;
}

export interface ChannelMeta {
  generation: number;
  seq: number;
}

export interface ChannelGap {
  generation: number;
  /** The seq the receiver expected. */
  expected: number;
  /** The seq it actually got. */
  received: number;
  /** True when a replay was attempted and the sender no longer had the messages. */
  unrecoverable: boolean;
}

export class SequencedChannel<T> implements Handler<never, never> {
  constructor(name: string, options?: SequencedChannelOptions);
  readonly wireType: string; // `st:ch:${name}`
  readonly generation: number; // outbound epoch
  send(value: T): void;
  newGeneration(): number;
  subscribe(listener: (value: T, meta: ChannelMeta) => void): () => void;
  onGap(listener: (gap: ChannelGap) => void): () => void;
}
```

One instance per channel per side; register it in `handlers` on both sides. Instances are
symmetric — each tracks its own outbound sequence and its own inbound expectation, so a
channel can flow both ways.

### Receiver state machine

Given inbound `{g, s, v}` against `(curGen, expected)`:

| Condition                        | Action                                                                    |
| -------------------------------- | ------------------------------------------------------------------------- |
| `g > curGen`                     | Adopt generation, clear any gap, deliver, `expected = s + 1`              |
| `g < curGen`                     | Drop (stale in-flight from before an epoch bump)                          |
| `g === curGen`, `s === expected` | Deliver, `expected++` (also how a replay resumes a gapped channel)        |
| `g === curGen`, `s > expected`   | **Gap.** Stop delivering. Request replay if configured, else emit `onGap` |
| `g === curGen`, `s < expected`   | Drop (duplicate)                                                          |

While gapped, same-generation messages are dropped rather than delivered out of order — the
guarantee is "every message in order, or an event". The channel leaves the gapped state on a
successful replay or on a new generation.

### Replay

With `replay: N` on the sender, a gap sends `{r: expected}` back. The sender resends every
buffered entry with `seq >= expected` (in order), and the receiver resumes at `expected` with
no application involvement. If the buffer no longer covers `expected`, the sender answers
`{m: expected}` and the receiver emits `onGap({unrecoverable: true})` and stays gapped until
the application drives a `newGeneration()`. Without `replay`, every gap goes straight to
`onGap`.

### Wire protocol

Sequenced messages ride the existing `{type: 'handler', wireType, payload}` envelope — no new
message type, and an old peer without the handler registered ignores them (`#handleHandlerMessage`
no-ops on an unknown `wireType`). Payload shapes, keyed short because they are per-message:

```jsonc
{"g": 3, "s": 128, "v": <value>}   // data
{"r": 129}                          // replay request (receiver → sender)
{"m": 129}                          // replay miss (sender → receiver)
```

Values pass through `toWire`/`fromWire`, so proxies, transfers, and custom handlers work
inside a channel payload.

### Composition

- **Batching** — channel messages queue through `#post`; a burst of `send()` calls in one
  microtask ships as one `postMessage` with order preserved, which is the point at graph scale.
- **Reconnect** — `disconnect()` resets inbound expectation and bumps the outbound generation,
  so the first message of the new session carries a fresh epoch and the peer resyncs from
  scratch instead of reporting a spurious gap.
- **`canHandle()` returns `false`** — the channel never claims values during serialization; it
  is a pure message channel. Registering it costs one extra `canHandle` call per serialized
  object.

### Size impact

Zero in the core barrel — separate entry point, like `handlers/streams.js`. Estimate ~500 B
brotli standalone.

### Tests

Node (`channel_test.ts`):

1. Ordered delivery; `meta.seq` increments; `meta.generation` is stable.
2. Gap detection: with a lossy endpoint that drops one message, the receiver emits `onGap`
   with the right `expected`/`received` and delivers nothing further.
3. Replay repairs a gap end-to-end, delivering the dropped message in order (`replay: N`).
4. Replay miss (drop older than the buffer) emits `onGap({unrecoverable: true})`.
5. `newGeneration()` resets seq, clears a gapped receiver, and delivers again.
6. Messages from an older generation arriving after a bump are dropped.
7. Duplicate seq is dropped.
8. Bidirectional use on a single pair of instances.
9. Batching: N `send()`s in one microtask = one `postMessage`, order preserved.
10. Reconnect: `reset()` + re-expose starts a new generation and the receiver resyncs without
    a gap event.
11. Multiple `subscribe()` listeners; unsubscribe stops one without affecting the other.

Browser (`channel_test.ts`): ordered delivery plus one gap case over a `MessageChannel`.

### Consumer migration note

The consumer's hand-rolled `{generation, seq}` header, its receiver-side gap check, and its
resync-request plumbing are replaced by `channel.send()` on the producer and
`channel.subscribe()` + `channel.onGap()` on the receiver. What stays is the domain half:
deciding what a resync sends, and calling `newGeneration()` when it does.

---

## Deferred (P2a and P3), with the hooks that make them cheap later

**P2a — buffering modes for hidden/paused endpoints.** Not implemented. It wants per-channel
buffering with (i) a re-produce mode that invokes a callback at flush time so the receiver
gets current truth, and (ii) per-key last-wins (`key: payload => string`). The natural home,
once P1a lands, is the notify path: `connection.pause()` / `resume()` with a per-channel
policy, since a notify is exactly the message class that is safe to coalesce (no promise is
waiting on it). It does **not** fall out of the P1 designs by itself — it needs a channel
identity on outbound notifies, which none of P1a–c introduce — so it is scoped here rather
than half-built. P1a's single send path and P1b's subscription registry are the two hooks it
would use.

**P3a — dev-mode serialization guard.** Independent of this work. An opt-in traversal (fold
into the existing `debug` option) that throws when a value crosses that no handler covers and
JSON would mangle (`Date`, `Map`, `Set`, `undefined` in arrays).

**P3b — pre-encoded fan-out.** Independent. Needs an encode/post split in `#post` so one
serialized payload can go to N endpoints.

---

## Milestones

Each milestone is code + node tests + browser tests + a changeset, and leaves `npm run build`,
`npm test`, `npm run lint`, `npm run format:check`, and `npm run checksize` green before the
next begins.

| M   | Contents                                                                                                |
| --- | ------------------------------------------------------------------------------------------------------- |
| M1  | P1a `notify()`; internal-symbol hook on remote proxies; minimal checksize input                         |
| M2  | P1b `subscribe()`/`Subscription`; `Connection._onReady`; P2b `ConnectionClosedError` + `Symbol.dispose` |
| M3  | P1c `SequencedChannel` + `./handlers/channel.js` export                                                 |
