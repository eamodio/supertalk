---
'@eamodio/supertalk-core': patch
---

Add `from` to `CallMessage` and a `Connection.callerSession` getter, so a handler can tell which peer session made the call it is servicing. `from` carries the sender's own session id — distinct from the existing `session` field, which is the sender's _belief_ about which session owns the target and can be stale or absent. `#sendCall` and `_sendNotify` always stamp outgoing calls with `from`.

`callerSession` is reliable **only synchronously** during dispatch: the value is set immediately before the target is invoked and restored as soon as that synchronous invocation returns — before anything it produced is awaited. A handler that needs the value must capture it into a local before its first `await`; reading it after suspending yields whatever dispatch is current by then, usually `undefined`. That narrow window is deliberate: holding the value across an awaited result would let two overlapping in-flight calls on one `Connection` observe each other's caller.

Attribution covers every path that synchronously runs user code on a peer's behalf: all four call shapes (get, set, direct call, and method call — including a method supplied by a getter or Proxy `get` trap), and the local property get performed when deserializing a peer's top-level property marker (a returned-but-unawaited remote property). The latter arrives in `return`/`resolve` frames, which carry no `from`, so it is attributed to the peer session learned from the handshake.

`callerSession` is `undefined` outside any dispatch, and for peers that predate `from`. It is attribution metadata, not authentication — `from` is peer-supplied and unverified, so it distinguishes a host's own concurrent clients from one another and must not be used as a trust boundary. Both wire fields stay optional, so peers that predate them are unaffected.
