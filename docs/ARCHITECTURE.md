# Supertalk Architecture

> **Status**: Draft — To be expanded as design solidifies

## Overview

Supertalk is structured in layers:

```
┌─────────────────────────────────────────────────────────┐
│                      User API                           │
│         expose(), wrap(), @service, @method             │
├─────────────────────────────────────────────────────────┤
│                    Proxy System                         │
│        Creates transparent proxies for remote access    │
├─────────────────────────────────────────────────────────┤
│                  Message Protocol                       │
│       Request/response, streaming, proxy lifecycle      │
├─────────────────────────────────────────────────────────┤
│                 Serialization Layer                     │
│    Structured clone, JSON, custom serializers           │
├─────────────────────────────────────────────────────────┤
│                   Transport Layer                       │
│   postMessage, MessagePort, HTTP, WebSocket             │
└─────────────────────────────────────────────────────────┘
```

---

## Core Concepts

### Endpoint

An `Endpoint` is an abstraction over any bidirectional communication channel:

```typescript
interface Endpoint {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent) => void,
  ): void;
  removeEventListener(
    type: 'message',
    listener: (event: MessageEvent) => void,
  ): void;
}
```

Implementations:

- `Worker` / `DedicatedWorkerGlobalScope`
- `MessagePort`
- `Window` (for iframes)
- `BroadcastChannel`
- HTTP adapter (request/response as messages)

### Services and Proxied Objects

A **service** is not a special concept — it's just an object that gets proxied across the communication boundary. The only differences from other proxied objects:

1. It's typically a singleton
2. It's often the "root" object of a connection

Conceptually, `expose(service, endpoint)` is equivalent to `send(proxy(service))` over an implicit root connection handler.

```typescript
// These are conceptually equivalent:

// 1. Exposing a service (syntactic sugar)
expose(new Calculator(), endpoint);

// 2. What's actually happening (pseudocode)
rootHandler.sendProxy(new Calculator());
```

**Implications:**

- Same proxy mechanism for services and any nested proxied object
- Methods = non-serializable function properties → proxied
- Serializable properties → cloned/sent
- No special cases for "top-level" vs nested objects

**Method enumeration:**

- Plain objects: own enumerable properties that are functions
- Class instances: walk prototype chain up to (not including) `Object.prototype`

### Remote

A `Remote<T>` is a proxy type that represents a remote service:

```typescript
type Remote<T> = {
  [K in keyof T]: T[K] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : Promise<T[K]>;
};
```

### AsyncProxy

An `AsyncProxy<T>` is a unified proxy type for objects marked with `proxy()`.
It provides an async interface on both sides of the connection:

```typescript
type AsyncProxy<T> = {
  [K in keyof T]: T[K] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : Promise<T[K]>;
};
```

The owning side can extract the underlying value with `getProxyValue()`. The
remote side can call methods and access properties asynchronously.

### Handle

A `Handle<T>` is an opaque reference type for objects marked with `handle()`.
Unlike proxies, handles provide no remote interface — they're pure tokens.

The owning side can extract the underlying value with `getHandleValue()`. The
remote side can only pass the handle around.

---

## Message Protocol

Messages between endpoints follow a request/response pattern:

```typescript
type Message =
  | CallMessage // Method invocation
  | ReturnMessage // Successful return
  | ThrowMessage // Error return
  | ReleaseMessage // Release a proxy
  | PromiseResolve // Resolve a proxied promise
  | PromiseReject; // Reject a proxied promise
```

### Initialization Handshake

When a connection is established:

1. `expose()` registers the root service and immediately sends a `ReturnMessage` with `id: 0` containing the root proxy and the sender's `session`
2. `wrap()` returns a `Promise<Remote<T>>` that resolves when the `id: 0` message arrives
3. If initialization fails, `expose()` sends a `ThrowMessage` with `id: 0`, and `wrap()` rejects

This handshake:

- Ensures the service is ready before calls are made
- Surfaces worker initialization errors to the caller
- Treats the root service as a normal proxy (not a special case)
- Carries the session id each side tags its outgoing messages with (see [Sessions](#sessions))

```typescript
// expose() sends immediately after setup:
{ type: 'return', id: 0, value: { __supertalk_type__: 'proxy', id: 0 }, session: 1234567890 }

// Or on error:
{ type: 'throw', id: 0, error: { name: 'Error', message: '...' } }
```

The failure form carries no `session`, so a side whose peer failed to
initialize never learns a peer session and simply omits the tag from anything
it sends — the same as talking to a peer that predates the field.

### Sessions

Each `Connection` holds a `session` id: a random, instance-unique token
(not a counter — a peer recreated as a fresh `Connection` over a persistent
endpoint would otherwise restart a counter at a value the other side still
considers current). `reset()` regenerates it.

Every wire proxy carries the session of the side that **owns** its target, in
the optional `s` field: a side sending a proxy to an object it registered
stamps its own session, and a side echoing a proxy back stamps the owner
session that proxy arrived with. Because the session rides on the proxy
rather than on a handshake, tagging protects both directions regardless of
which side called `expose()` — including a `wrap()`-only side, which never
exposes and so never sends a handshake of its own.

Receiving a wire proxy freezes its owner session onto the proxy object, and
every `CallMessage` or `ReleaseMessage` sent through that proxy carries that
frozen tag. Proxy identity is per **(owner session, id)**, not per id: when a
`reset()` lets the peer reclaim an id for a different object, the incoming
proxy **mints a new, distinct proxy object** and the retained one keeps its
original tag, so calls through it are still rejected while the new one works.
Both live in the cache at once under their own owner sessions, which is what
stops either displacing the other — a stale call's arguments are deserialized
before the call is rejected (so the resources they carry aren't stranded), so
a stale wire proxy naming a reused id does reach the cache, and must not be
able to evict the live one.

The root (`id: 0`) is the one exception, and the one id re-sent on every
handshake. It is always and only the expose-side root — `expose()` registers
it first, and the wrap side's own locals are odd — and it names "the peer's
root service", a stable role rather than one particular object. A re-handshake
therefore re-keys the retained root to the new owner session instead of
superseding it, so a consumer holding the root keeps working across a peer
`reset()` even if it never awaits readiness again.

An echo of one of our own ids resolves to the live local object only when it
carries our current session; a stale echo does not, since that id may since
have been reused.

The receiver drops anything tagged with a session other than its current one:
a `CallMessage` is answered with a `ThrowMessage` (or silently dropped when
one-way), and a `ReleaseMessage` is ignored. This catches what a
missing-target check cannot — after a `reset()` ids restart from 0, so a
message from before the reset can name an id that is not missing but now
belongs to an unrelated object.

Both `session` and `s` are optional on the wire. Peers that predate them send
neither; a wire proxy with no `s` falls back to the session learned from the
handshake, and an absent `session` skips the receiver's check rather than
counting as a mismatch.

```typescript
interface WireProxy {
  [WIRE_TYPE]: 'proxy';
  id: number;
  o: boolean; // Opaque (handle) flag
  s?: number; // Session of the side that OWNS the target
}
```

### Message IDs

- `id: 0` is reserved for the initialization handshake
- Subsequent message IDs start from 1 and increment

```typescript
interface CallMessage {
  type: 'call';
  id: number;
  target: number; // Proxy ID (0 = root service)
  action: 'call' | 'get';
  method?: string;
  args: WireValue[];
  session?: number; // Peer session `target` is believed to live in
}

interface ReturnMessage {
  type: 'return';
  id: number;
  value: WireValue;
  session?: number; // Sender's own session; handshake (id 0) only
}

interface ReleaseMessage {
  type: 'release';
  id: number;
  session?: number; // Peer session `id` is believed to live in
}
```

---

## Proxy System

### Creating Proxies

When `wrap()` is called, we create a Proxy that:

1. Intercepts property access → builds path
2. Intercepts function calls → sends CallMessage
3. Returns Promises that resolve when ReturnMessage arrives

### Proxy Lifecycle

```
┌──────────────┐         ┌──────────────┐
│   Sender     │         │   Receiver   │
├──────────────┤         ├──────────────┤
│              │ create  │              │
│  Real Object ├────────►│    Proxy     │
│              │         │  (WeakRef)   │
│              │         │              │
│              │ release │              │
│  (released)  │◄────────┤  (GC'd)      │
└──────────────┘         └──────────────┘
```

- Sender retains real object until release
- Receiver holds WeakRef to proxy
- FinalizationRegistry notifies sender when proxy is GC'd

---

## Serialization

### Structured Clone (postMessage)

For worker/iframe communication, use browser's structured clone:

- Automatically handles: primitives, arrays, objects, Map, Set, Date, RegExp, ArrayBuffer, etc.
- Transferables: ArrayBuffer, MessagePort, ReadableStream, etc.
- NOT supported: functions, Proxies, DOM nodes

### JSON (HTTP)

For HTTP transport:

- Default: `JSON.stringify` / `JSON.parse`
- Enhanced: pluggable serializers (superjson for Date, Map, Set, etc.)

### Custom Serialization

For types that need special handling:

```typescript
interface Serializable<T, S> {
  [serializeSymbol](value: T): S;
  [deserializeSymbol](serialized: S): T;
}
```

---

## Transport Layer

### postMessage Transport

```typescript
class PostMessageTransport implements Transport {
  constructor(endpoint: Endpoint) {}

  send(message: Message, transfer?: Transferable[]): void;
  onMessage(handler: (message: Message) => void): void;
}
```

### HTTP Transport

> **TODO**: Design HTTP transport

Considerations:

- Request/response mapping to call/return messages
- Long-polling or SSE for server-initiated messages
- WebSocket option for bidirectional

---

## Open Questions

1. **Decorator metadata**: How much runtime metadata do we need? Can we infer from types alone?

2. **Proxy granularity**: When should nested objects become separate proxies vs. cloned?

3. **Stream backpressure**: How do we handle backpressure across the boundary?

4. **Error serialization**: How do we serialize Error objects with stack traces?

5. **Cancellation**: How does AbortSignal work across the boundary?

6. **Batching**: When/how do we batch multiple calls?

---

## Implementation Phases

See [ROADMAP.md](ROADMAP.md) for detailed implementation plan.
