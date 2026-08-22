---
'@eamodio/supertalk-core': minor
'@eamodio/supertalk': minor
---

Add `notify()` for one-way, fire-and-forget calls against a remote proxy.

A notify sends a single wire message — a `CallMessage` stamped with the reserved
sentinel `id: -1` — and never waits for or registers a response, so an event
emission or a fire-and-forget write costs one message instead of two, and there
is no unawaited promise to leak as an unhandled rejection. Errors on either side
(serialization failures on the sender, a throwing handler on the receiver)
surface through the connection's `logger` option instead.

An old (pre-`notify()`) receiver still executes the call correctly; it just
replies with an unneeded `return`/`throw` that the new sender silently drops,
degrading to today's two-message cost rather than losing the call.
