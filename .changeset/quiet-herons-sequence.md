---
'@eamodio/supertalk-core': minor
'@eamodio/supertalk': minor
---

Add `SequencedChannel` for ordered delivery with gap detection and generations.

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
