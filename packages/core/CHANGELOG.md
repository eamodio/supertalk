# @supertalk/core

## 0.1.0

### Minor Changes

- 2a8b195: Add `notify()` for one-way, fire-and-forget calls against a remote proxy.

  A notify sends a single wire message — a `CallMessage` stamped with the reserved
  sentinel `id: -1` — and never waits for or registers a response, so an event
  emission or a fire-and-forget write costs one message instead of two, and there
  is no unawaited promise to leak as an unhandled rejection. Errors on either side
  (serialization failures on the sender, a throwing handler on the receiver)
  surface through the connection's `logger` option instead.

  An old (pre-`notify()`) receiver still executes the call correctly; it just
  replies with an unneeded `return`/`throw` that the new sender silently drops,
  degrading to today's two-message cost rather than losing the call.

- 46ad2d7: Add `SequencedChannel` for ordered delivery with gap detection and generations.

  `SequencedChannel<T>`, from the new `@eamodio/supertalk-core/handlers/channel.js`
  entry point, guarantees every value is delivered in order or an `onGap` event
  fires — never both, and never neither. Each side registers one instance per
  channel in `handlers`; instances are symmetric, so a single pair can carry
  traffic both ways. `newGeneration()` bumps a sender-side epoch, invalidating
  stale in-flight messages and resetting the receiver's gap state.

  `onGap` fires exactly once per gap, only when the channel cannot self-heal: on
  detecting a gap the receiver requests a replay and waits; if the sender still
  has the messages (`{replay: N}`), they resend and delivery resumes with no gap
  event; only a replay miss surfaces `onGap`. The cost is one round trip of
  latency before a genuine gap is reported, and — if the replay request or its
  response is lost in transit — the next message that advances past the gap
  re-issues the request, so recovery costs at most one extra message of delay
  rather than stalling until the next connect.

  It is a separate entry point, like `handlers/streams.js`: importing it costs
  nothing in the main bundle unless a consumer actually uses it.

- d874de8: Add `subscribe()` for synchronous subscription handles, and `ConnectionClosedError` for deliberate-teardown call settlement.

  `subscribe(target, subscriber)` returns a `Subscription` handle immediately and
  buffers the wire subscribe call internally until the connection's handshake
  completes, so a consumer can subscribe from a constructor without awaiting
  anything. The library re-invokes the subscriber on every subsequent handshake
  (after a `reset()` + `waitForReady()` reconnect), so the application no longer
  re-subscribes by hand; `subscription.unsubscribe()` releases the remote side
  and cancels resubscription, and `Subscription` implements `Symbol.dispose` for
  `using`.

  `close()` and `reset()` now reject in-flight calls and promises with a new
  `ConnectionClosedError` (carrying `reason: 'closed' | 'reset'`) instead of a
  bare `Error`, so consumers can filter deliberate teardown from a real failure.
  `subscribe()` relies on this internally to swallow teardown/reset failures
  rather than logging them. `Connection` also gains `[Symbol.dispose]()` (calls
  `close()`) for `using`.

## 0.0.6

### Patch Changes

- 868e7b8: Fork and republish under @eamodio scope

## 0.0.5

### Patch Changes

- 79b0d6f: Fix an infinite loop bug with payloads with cycles

## 0.0.4

### Patch Changes

- 75d8c36: Only recursively traverse plain objects so that natively cloneable objects like Maps are handled correctly

## 0.0.3

### Patch Changes

- e9bc8f6: ### Added
  - **`handle()` and `getHandleValue()`** for opaque handle passing — pass
    references across the boundary without exposing an async interface. Handles
    are lightweight marker objects (not JS Proxies) that can be passed back to the
    owning side and dereferenced.
  - **Local proxies work like remote proxies** — `proxy()` now returns an
    `AsyncProxy<T>` that provides the same async interface locally and remotely.
    Methods return promises and properties are accessible via `await`. This means
    the same code works both on the local side or the remote side.
  - **`getProxyValue()`** to extract the underlying value from an `AsyncProxy` on
    the owning side

  ### Changed
  - **Proxies don't auto-unwrap** — When a proxy is sent back across the worker
    boundary, it stays as a proxy rather than being unwrapped to the original
    value. Use `getProxyValue()` on the owning side to access the underlying
    value. This improved the typing of remote APIs and enables APIs that are
    compatible between local and remote sides
  - **Reduced bundle size** from ~2.6 kB to ~2.4 kB brotli through internal
    optimizations

  ### Fixed
  - **Class instances no longer throw in debug mode**: they pass through to
    structured clone (which will clone data but lose methods), matching the
    behavior of shallow mode
  - **Debug mode detects more invalid nested values**: now throws
    `NonCloneableError` for nested `proxy()` and `transfer()` markers in addition
    to functions and promises when `nestedProxies` is not enabled

## 0.0.2

### Patch Changes

- cb844ea: Add nodeEndpoint() wrapper function for Node Worker support

## 0.0.1

### Patch Changes

- 81a9122: Initial release
