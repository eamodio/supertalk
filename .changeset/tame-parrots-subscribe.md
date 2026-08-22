---
'@eamodio/supertalk-core': minor
'@eamodio/supertalk': minor
---

Add `subscribe()` for synchronous subscription handles, and `ConnectionClosedError` for deliberate-teardown call settlement.

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
