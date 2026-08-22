# @supertalk/core

[![CI](https://github.com/eamodio/supertalk/actions/workflows/ci.yml/badge.svg)](https://github.com/eamodio/supertalk/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@eamodio/supertalk-core.svg)](https://www.npmjs.com/package/@eamodio/supertalk-core)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

> [!WARNING]
> This is a pre-release package under active development. APIs may change without
> notice between versions.

A type-safe, unified communication library for Web Workers, Iframes, and Node.js
worker threads.

## Overview

**Supertalk turns workers' low-level message passing into a high-level,
type-safe RPC layer—so you can call methods, pass callbacks, and await promises
as if they were local.**

Supertalk is built to be a joy to use and deploy:

- **Type-safe:** Your IDE knows exactly what's proxied vs cloned
- **Ergonomic:** Callbacks, promises, and classes just work
- **Bidirectional:** The same types and patterns work in both directions
- **Fast & small:** ~2.4 kB brotli-compressed, zero dependencies
- **Composable & extendable:** Non-global configuration, nested objects,
  services are just classes, composable transport handlers
- **Standard modules:** Some people call them "ESM". We don't publish CJS.
- **Modern JavaScript:** Published as ES2024, targeting current browsers and
  Node.js 20+

## Installation

```bash
npm install @supertalk/core
```

## Quick Start

`worker.ts` (exposed side):

```ts
import {expose} from '@supertalk/core';

const service = {
  add(a: number, b: number): number {
    return a + b;
  },
  async fetchData(url: string): Promise<string> {
    const res = await fetch(url);
    return res.text();
  },
};

expose(service, self);
```

`main.ts` (wrapped side):

```ts
import {wrap} from '@supertalk/core';

const worker = new Worker('./worker.ts');
const remote = await wrap<typeof service>(worker);

// Methods become async
const result = await remote.add(1, 2); // 3
```

## Core Features

### Automatic proxying of functions and promises

Functions and promises passed as **top-level arguments or return values** are
always proxied:

```ts
// Exposed side
const service = {
  forEach(items: number[], callback: (item: number) => void) {
    for (const item of items) {
      callback(item); // Calls back to wrapped side
    }
  },
};
```

```ts
// Wrapped side
await remote.forEach([1, 2, 3], (item) => {
  console.log(item); // Runs locally
});
```

All functions are transformed into promise-returning async functions on the
wrapped side. Proxies are released from memory when they're no longer in use by
the wrapped side.

### One-way calls with `notify()`

Every regular call is request/response — even when nothing needs the reply.
`notify()` wraps a remote proxy (or a callback proxy received as an argument)
so calling it sends a single wire message with no response and returns `void`,
instead of a `Promise` nobody awaits:

```ts
import {notify} from '@supertalk/core';

// Method calls on a remote object
notify(remote).reportSelection(ids); // void — one wire message, no ack
```

```ts
// A callback proxy received from the other side (the event-emission case)
class Service {
  onDidChange(cb: (e: Change) => void) {
    const emit = notify(cb); // hoist once
    this.#listeners.add(emit);
    return () => this.#listeners.delete(emit);
  }
}
```

Hoist the notifier out of hot loops, the same way you'd hoist `remote.method`.
Errors — a throwing handler, a serialization failure — are reported through
the `logger` option on whichever side produced them, never as a rejection.
`notify()` throws `TypeError` if `target` isn't a remote proxy (a `handle()`,
for example, has no callable surface). Don't pass per-call resources like
`AbortSignal` through `notify()`: a handler that releases on call settlement
never gets told to, since a notify never settles.

### Subscriptions with `subscribe()`

Subscribing the plain way is `await remote.onEvent(cb)`, returning a
`Promise<Unsubscribe>`. That races teardown and reconnect: every call site
needs a staleness guard, and after a reconnect the application has to
re-subscribe everything by hand. `subscribe()` returns a handle synchronously
and buffers the wire subscribe call until the connection's handshake
completes, so you can subscribe from a constructor and never `await`
anything:

```ts
import {subscribe} from '@supertalk/core';

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

The `subscriber` thunk receives the current root proxy and does whatever
subscribing means for that service; its return value, if a function, is
treated as the remote unsubscribe. The library re-invokes it on every
successful handshake — including after a `reset()` + `waitForReady()`
reconnect — so the subscriber must be idempotent and must always re-derive
whatever it subscribes on from the `remote` it's given. Only subscriptions
anchored on the **root** proxy resurrect this way; a proxy from a dead
session is gone by definition. When passing a bare `Connection`, the
application still has to drive `waitForReady()` (or use `wrap()`) — that
call is what signals ready and triggers delivery.

```ts
interface Subscription extends Disposable {
  unsubscribe(): void; // idempotent; releases the remote side
  readonly closed: boolean;
  readonly ready: Promise<void>; // settles once the first subscribe lands
}
```

`unsubscribe()` is idempotent and works even before the initial subscribe
call has landed — the pending call's returned unsubscribe is invoked as soon
as it arrives instead of being stored. `Subscription` implements
`Symbol.dispose`, so `using` works too:

```ts
using rows = subscribe(connection, (remote) => remote.onDidChangeRows(cb));
```

`Subscription extends Disposable`, so a consuming project's `lib` must include
`esnext.disposable` (`@types/node` pulls it in automatically).

Failures — a throwing subscriber, a rejected `target` promise — are reported
through the `logger` option and surface on `ready`. The exceptions are the
`ConnectionClosedError`s that `close()`/`reset()` reject in-flight work with:
neither is logged, since both mean deliberate teardown. A `reset()` is
swallowed entirely — the next reconnect re-issues automatically — while a
`close()` also settles a still-pending `ready` with the error, so awaiting
it can never hang.

### Object proxying with `proxy()`

Objects are cloned by default. This works well for immutable data objects, but
not for all cases. The `proxy()` function marks an object as needing to be
proxied instead of cloned.

**Use `proxy()` when returning:**

1. **Mutable objects** — The remote side should see updates
2. **Large graphs** — Avoid cloning expensive data structures
3. **Class instances with methods** — Preserve the prototype API

`worker.ts`:

```ts
import {expose, proxy} from '@supertalk/core';

export class Widget {
  count = 42;
  sayHello() {
    return 'hello';
  }
}

expose(
  {
    createWidget() {
      return proxy(new Widget()); // Explicitly proxied
    },
  },
  self,
);
```

`main.ts`:

```ts
const service = await wrap(worker);
const widget = await service.createWidget();

// The proxy provides async access to properties and methods
const count = await widget.count;
const hello = await widget.sayHello();
```

### Opaque Handles with `handle()`

When you need to pass a reference without exposing any remote interface, use
`handle()`. Handles are opaque tokens — they can be passed around and back, but
provide no way to access properties or methods remotely.

```ts
import {expose, handle, getHandleValue} from '@supertalk/core';

class Session {
  constructor(public id: string) {}
}

const service = {
  createSession(id: string) {
    return handle(new Session(id)); // Opaque handle
  },
  getSessionId(session: Handle<Session>) {
    // Extract the value on the owning side
    return getHandleValue(session).id;
  },
};
```

```ts
// Client side
const session = await remote.createSession('abc'); // Handle<Session>
// session is opaque — no property access
const id = await remote.getSessionId(session); // Pass it back
```

### Extracting Values from Proxies and Handles

Both `proxy()` and `handle()` create wrappers that work the same on both sides.
On the side that created the proxy/handle, you can extract the underlying value:

```ts
import {proxy, getProxyValue, handle, getHandleValue} from '@supertalk/core';

const service = {
  // Return a proxied widget
  createWidget() {
    return proxy(new Widget());
  },

  // Accept a proxy and extract its value
  updateWidget(widget: AsyncProxy<Widget>) {
    const w = getProxyValue(widget); // Only works on owning side
    w.refresh();
  },

  // Same pattern works for handles
  processSession(session: Handle<Session>) {
    const s = getHandleValue(session);
    return s.data;
  },
};
```

### Nested Objects & Proxies

Supertalk supports two modes for handling nested values:

- **Shallow mode** (default): Maximum performance, only top-level function
  arguments and return values are proxied. Nested functions/promises fail with
  `DataCloneError`.
- **Nested mode** (`nestedProxies: true`): Full payload traversal. Functions and
  promises anywhere in the graph are auto-proxied.

```ts
// On exposed side
expose(service, self, {nestedProxies: true});

const service = {
  getData() {
    return {
      process: (x) => x * 2, // auto-proxied (function)
      widget: proxy(new Widget()), // explicitly proxied (class)
    };
  },
};

// On wrapped side
const remote = await wrap<typeof service>(worker, {nestedProxies: true});
const data = await remote.getData();
await data.process(5); // 10
```

### Transferables with `transfer()`

Use `transfer()` to mark values like `ArrayBuffer`, `MessagePort`, or streams to
be transferred rather than cloned.

```ts
import {transfer} from '@supertalk/core';

const service = {
  getBuffer(): ArrayBuffer {
    const buf = new ArrayBuffer(1024);
    return transfer(buf); // Zero-copy transfer
  },
};
```

### Value Handling Reference

When values cross the communication boundary:

| Value Type                                       | Shallow Mode   | Nested Mode    |
| ------------------------------------------------ | -------------- | -------------- |
| Primitives                                       | ✅ Cloned      | ✅ Cloned      |
| Plain objects `{...}`                            | ✅ Cloned      | ✅ Cloned      |
| Arrays                                           | ✅ Cloned      | ✅ Cloned      |
| Functions (top-level)                            | 🛜 Proxied     | 🛜 Proxied     |
| Functions (nested)                               | ❌ Error       | 🛜 Proxied     |
| Promises (top-level return)                      | 🛜 Proxied     | 🛜 Proxied     |
| Promises (nested)                                | ❌ Error       | 🛜 Proxied     |
| `proxy()` wrapped values (top-level)             | 🛜 Proxied     | 🛜 Proxied     |
| `proxy()` wrapped values (nested)                | ❌ Error       | 🛜 Proxied     |
| `handle()` wrapped values (top-level)            | 🔒 Handle      | 🔒 Handle      |
| `handle()` wrapped values (nested)               | ❌ Error       | 🔒 Handle      |
| `transfer()` wrapped values (top-level)          | 📦 Transferred | 📦 Transferred |
| `transfer()` wrapped values (nested)             | ❌ Error       | 📦 Transferred |
| Class instances & objects with custom prototypes | ⚠️ Cloned\*    | ⚠️ Cloned\*    |
| Other structured cloneable objects               | ✅ Cloned      | ✅ Cloned      |

\* _Class instances are cloned via structured clone (losing methods) unless
wrapped in `proxy()`._

## API Reference

### Core Functions

#### `expose(target, endpoint, options?)`

Exposes an object or function to the other side.

- `target`: The service object or function to expose.
- `endpoint`: The `Worker`, `MessagePort`, `Window`, or compatible interface.
- `options`: Connection options (see below).

#### `wrap<T>(endpoint, options?)`

Connects to an exposed service and returns a proxy.

- `endpoint`: The `Worker`, `MessagePort`, `Window`, or compatible interface.
- `options`: Connection options (see below).
- Returns: `Promise<Remote<T>>`

#### `proxy(value)`

Marks an object to be proxied rather than cloned. Returns an `AsyncProxy<T>`
that provides async access on both sides. Use this for:

- Class instances with methods
- Mutable objects that should be shared
- Large objects to avoid cloning costs

#### `handle(value)`

Marks an object as an opaque handle. Returns a `Handle<T>` that can be passed
around but provides no remote interface. Use this for:

- Session tokens or identifiers
- References to expensive objects
- Graph nodes where you don't want to expose internals

#### `getProxyValue(proxy)` / `getHandleValue(handle)`

Extracts the underlying value from an `AsyncProxy` or `Handle`. Only works on
the side that created the proxy/handle — throws `TypeError` on the remote side.

#### `transfer(value, transferables?)`

Marks a value to be transferred (zero-copy) rather than cloned.

- `value`: The value to send (e.g. `ArrayBuffer`, `MessagePort`).
- `transferables`: Optional array of transferables. If omitted, `value` is assumed to be the transferable.

### Options

```ts
interface Options {
  /** Enable nested proxy handling (default: false) */
  nestedProxies?: boolean;
  /**
   * Enable debug mode for better error messages.
   * Throws NonCloneableError with the exact path for nested functions,
   * promises, proxy() markers, and transfer() markers that would fail
   * without nestedProxies: true.
   */
  debug?: boolean;
  /** Custom handlers for serializing/deserializing specific types */
  handlers?: Array<Handler>;
}
```

### Handlers

Handlers provide pluggable serialization for custom types or streams.

**Stream Handler Example:**

```ts
import {streamHandler} from '@supertalk/core/handlers/streams.js';

expose(service, self, {handlers: [streamHandler]});
const remote = await wrap<typeof service>(worker, {handlers: [streamHandler]});
```

**Custom Handlers:**

Handlers let you control how specific types are serialized. Each handler implements:

- `wireType` — Unique identifier (e.g., `'app:my-type'`)
- `canHandle(value)` — Returns `true` if this handler should process the value
- `toWire(value, ctx)` — Serialize the value, using `ctx.toWire()` for nested values
- `fromWire(wire, ctx)` — Deserialize, using `ctx.fromWire()` for nested values

```ts
import {WIRE_TYPE, type Handler} from '@supertalk/core';

// Handler that clones Maps by converting to/from arrays
const mapHandler: Handler<Map<unknown, unknown>, {entries: unknown[]}> = {
  wireType: 'app:map',

  canHandle: (v): v is Map<unknown, unknown> => v instanceof Map,

  toWire(map, ctx) {
    return {
      [WIRE_TYPE]: 'app:map',
      entries: [...map.entries()].map(([k, v]) => [
        ctx.toWire(k),
        ctx.toWire(v),
      ]),
    };
  },

  fromWire(wire, ctx) {
    return new Map(
      wire.entries.map(([k, v]) => [ctx.fromWire(k), ctx.fromWire(v)]),
    );
  },
};
```

**Subscription Handlers:**

For handlers that need to send updates outside of RPC calls (like signals or observables), the handler lifecycle provides messaging support:

- `connect(ctx)` — Called when attached; provides `ctx.sendMessage()` for sending updates
- `onMessage(payload)` — Called when a message arrives for this handler's wireType
- `disconnect()` — Called when the connection closes; clean up resources

See [@supertalk/signals](../signals) for a complete example.

### TypeScript Types

#### `Remote<T>`

The primary type for service proxies returned by `wrap()`. Transforms all
methods to async and properties to `Promise<T>`.

```ts
const remote = await wrap<MyService>(worker);
// remote has type Remote<MyService>
```

#### `AsyncProxy<T>`

The unified proxy type returned by `proxy()`. Works the same on both sides of a
connection — the remote side gets async access to properties and methods, while
the owning side can extract the underlying value with `getProxyValue()`.

```ts
// Service returns an AsyncProxy
createWidget(): AsyncProxy<Widget> {
  return proxy(new Widget());
}

// Client receives the same type
const widget = await remote.createWidget(); // AsyncProxy<Widget>
await widget.name;       // Property access is async
await widget.activate(); // Method calls are async
```

#### `Handle<T>`

An opaque reference type returned by `handle()`. Provides no remote interface —
useful for session tokens, expensive objects, or graph nodes that shouldn't
expose their internals.

```ts
// Service returns a Handle
createSession(): Handle<Session> {
  return handle(new Session());
}

// Client can only pass it back
const session = await remote.createSession(); // Handle<Session>
// session.foo — not allowed, handles are opaque
await remote.useSession(session); // Pass it back
```

## Advanced Usage

### Debugging

Debugging `DataCloneError` can be tricky. Supertalk provides a `debug` option
that traverses your data before sending and throws a `NonCloneableError` with
the exact path to the problematic value.

```ts
const remote = await wrap<Service>(endpoint, {debug: true});
```

### Memory Management

Proxied objects are tracked with registries on both sides.

- **Source side**: Holds strong references until released.
- **Consumer side**: Holds weak references; when GC'd, notifies source to
  release.

### Node.js Worker Threads

For Node.js `worker_threads`, use the `nodeEndpoint` adapter to convert the
Node-style event API (`on`/`off`) to the browser-style API
(`addEventListener`/`removeEventListener`).

**main.ts:**

```ts
import {wrap} from '@supertalk/core';
import {nodeEndpoint} from '@supertalk/core/node.js';
import {Worker} from 'node:worker_threads';

const worker = new Worker('./worker.js');
const remote = await wrap<MyService>(nodeEndpoint(worker));

const result = await remote.add(1, 2);
worker.terminate();
```

**worker.ts:**

```ts
import {expose} from '@supertalk/core';
import {parentPort} from 'node:worker_threads';

const service = {
  add(a: number, b: number) {
    return a + b;
  },
};

// parentPort is a MessagePort which has addEventListener/removeEventListener
expose(service, parentPort!);
```

## Benchmarks

Supertalk vs Comlink vs Supertalk with `nestedProxies: true`, measured in
ops/sec (higher is better). Node.js `worker_threads` with `MessageChannel`.

| Benchmark                    | Supertalk | Comlink | ST vs Comlink | Supertalk w/ <br> nestedProxies | nested vs <br> shallow |
| ---------------------------- | --------: | ------: | ------------: | ------------------------------: | ---------------------: |
| Simple String Echo           |   168,597 |  94,543 |         1.78x |                         173,684 |                  1.03x |
| Multiple Arguments (4 nums)  |   163,503 |  84,546 |         1.93x |                         163,420 |                     1x |
| Large Object (~10KB)         |    26,257 |  23,714 |         1.11x |                          12,886 |                  0.49x |
| Large Array (10,000 items)   |       313 |     309 |         1.01x |                             163 |                  0.52x |
| Callback (proxy function)    |     3,742 |   3,469 |         1.08x |                           3,607 |                  0.96x |
| Multiple Callbacks (3 funcs) |     1,748 |   1,576 |         1.11x |                           1,856 |                  1.06x |
| Rapid Sequential (20x burst) |   190,164 | 103,191 |         1.84x |                         179,378 |                  0.94x |

**Notes:**

- Supertalk appears to have lower per-call and per-object overhead, which makes
  it faster in the multiple call and multiple argument cases, and similar in the
  one large object or array cases.
- For simple calls and bursts, Supertalk is ~1.8-1.9x faster than Comlink
- `nestedProxies` mode adds traversal overhead for large payloads. The
  performance impact ranges from negligible for small objects to 2x slower for
  large graphs and arrays.
- Run the benchmarks with: `npm run bench -w @supertalk/core`

## Background

### Why Supertalk?

Workers are great for offloading work, but the raw `postMessage` API is
difficult:

- No built-in request/response (one-way only)
- No error propagation
- No functions or promises (DataCloneError)
- Manual lifetime management
- Manual transfer lists

Supertalk handles all of this: RPC layer, transparent proxying, automatic
lifetime management, and deep traversal.

### Comparison to Comlink

Supertalk is inspired by Comlink but differs in key ways:

- **Automatic proxying:** Functions/promises are auto-proxied.
- **Nested support:** `nestedProxies` mode allows proxies anywhere in the
  payload.
- **Debug mode:** Reports exactly where non-serializable values are.
- **Symmetric:** Both ends use the same `Connection` class.
- **No MessagePorts:** Uses the worker/window directly, making it lighter.

## License

MIT
