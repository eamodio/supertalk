---
'@eamodio/supertalk-core': patch
---

Retain the target proxy for a notifier's lifetime — `notify()` previously captured only the proxy's id, so a caller that kept just the notifier (the event-callback pattern) left the proxy to GC, whose `release` handshake made the peer forget the callback and silently drop every later notify
