/**
 * Tests for Connection.reset() — reconnection support.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel, type MessagePort} from 'node:worker_threads';
import {Connection} from '../../lib/connection.js';
import {notify} from '../../lib/notify.js';
import {proxy} from '../../lib/protocol.js';
import {WIRE_TYPE} from '../../lib/constants.js';
import type {Handler, HandlerConnectionContext} from '../../lib/types.js';

/**
 * Helper: expose a service on one port, wrap from the other.
 * Returns the host Connection, the remote proxy, and the wrap Connection.
 */
async function setupPair<T extends object>(
  service: T,
  hostPort: MessagePort,
  wrapPort: MessagePort,
): Promise<{host: Connection; remote: T; wrap: Connection}> {
  const host = new Connection(hostPort);
  const wrap = new Connection(wrapPort);

  host.expose(service);
  const remote = (await wrap.waitForReady()) as T;

  return {host, remote, wrap};
}

void suite('Connection.reset()', () => {
  void test('reset and re-expose allows new wrap session', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const service = {
      greet(name: string): string {
        return `Hello, ${name}!`;
      },
    };

    // Session 1
    const host = new Connection(ch1.port1);
    host.expose(service);
    const wrap1 = new Connection(ch1.port2);
    const remote1 = (await wrap1.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    const result1 = await remote1.greet('Alice');
    assert.strictEqual(result1, 'Hello, Alice!');

    // Tear down session 1's wrap side
    wrap1.close();
    ch1.port2.close();

    // Reset host with new endpoint for session 2
    host.reset(ch2.port1);
    host.expose(service);

    const wrap2 = new Connection(ch2.port2);
    const remote2 = (await wrap2.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    const result2 = await remote2.greet('Bob');
    assert.strictEqual(result2, 'Hello, Bob!');

    // Cleanup
    wrap2.close();
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('reset rejects pending calls', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      neverResolve(): Promise<string> {
        return new Promise(() => {
          // intentionally never resolves
        });
      },
    };

    const {host, remote, wrap} = await setupPair(service, port1, port2);

    // Start a call that will never resolve
    const pendingCall = remote.neverResolve();

    // Give the call message time to be sent
    await new Promise((r) => setTimeout(r, 10));

    // Reset the wrap side (which has the pending call)
    wrap.reset();

    // The pending call should reject
    await assert.rejects(pendingCall, (err: Error) => {
      assert.strictEqual(err.message, 'Connection reset');
      return true;
    });

    host.close();
    port1.close();
    port2.close();
  });

  void test('reset clears local registries — new services are used', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const serviceA = {
      identify(): string {
        return 'A';
      },
    };
    const serviceB = {
      identify(): string {
        return 'B';
      },
    };

    // Session 1: expose service A
    const host = new Connection(ch1.port1);
    host.expose(serviceA);
    const wrap1 = new Connection(ch1.port2);
    const remote1 = (await wrap1.waitForReady()) as typeof serviceA;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote1.identify(), 'A');

    wrap1.close();
    ch1.port2.close();

    // Session 2: reset and expose service B
    host.reset(ch2.port1);
    host.expose(serviceB);
    const wrap2 = new Connection(ch2.port2);
    const remote2 = (await wrap2.waitForReady()) as typeof serviceB;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote2.identify(), 'B');

    wrap2.close();
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('reset after close re-enables the connection', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const service = {value: 42};

    // Session 1
    const host = new Connection(ch1.port1);
    host.expose(service);
    const wrap1 = new Connection(ch1.port2);
    const remote1 = (await wrap1.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    assert.strictEqual(await remote1.value, 42);

    // Close both sides of session 1
    wrap1.close();
    host.close();
    ch1.port1.close();
    ch1.port2.close();

    // Reset with new endpoint should re-enable
    host.reset(ch2.port1);
    host.expose(service);
    const wrap2 = new Connection(ch2.port2);
    const remote2 = (await wrap2.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    assert.strictEqual(await remote2.value, 42);

    wrap2.close();
    host.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('reset without endpoint preserves listener', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      ping(): string {
        return 'pong';
      },
    };

    // Session 1
    const host = new Connection(port1);
    host.expose(service);
    const wrap1 = new Connection(port2);
    const remote1 = (await wrap1.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote1.ping(), 'pong');

    // Reset host without new endpoint (not closed, so listener stays)
    host.reset();
    host.expose(service);

    // The wrap side needs a fresh connection on the same port.
    // Since the port is still open, reset the wrap side too.
    wrap1.reset();
    const remote2 = (await wrap1.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote2.ping(), 'pong');

    wrap1.close();
    host.close();
    port1.close();
    port2.close();
  });

  void test('reset cycles handler connect/disconnect', () => {
    const {port1, port2} = new MessageChannel();

    let connectCount = 0;
    let disconnectCount = 0;
    let lastCtx: HandlerConnectionContext | undefined;

    const trackingHandler: Handler = {
      wireType: 'test:tracking',
      canHandle: (_v: unknown): _v is unknown => false,
      toWire: (v) => v as object,
      connect(ctx) {
        connectCount++;
        lastCtx = ctx;
      },
      disconnect() {
        disconnectCount++;
      },
    };

    // Constructor calls connect()
    const conn = new Connection(port1, {handlers: [trackingHandler]});
    assert.strictEqual(connectCount, 1);
    assert.strictEqual(disconnectCount, 0);
    assert.ok(lastCtx !== undefined);

    const firstCtx = lastCtx;

    // Reset should disconnect then reconnect
    conn.reset();

    assert.strictEqual(connectCount, 2);
    assert.strictEqual(disconnectCount, 1);
    assert.ok(lastCtx !== firstCtx, 'Should get a new context after reset');

    // Second reset
    conn.reset();

    assert.strictEqual(connectCount, 3);
    assert.strictEqual(disconnectCount, 2);

    conn.close();
    port1.close();
    port2.close();
  });

  void test('reset with endpoint swap', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const service = {
      echo(msg: string): string {
        return msg;
      },
    };

    // Session 1 on ch1
    const host = new Connection(ch1.port1);
    host.expose(service);
    const wrap1 = new Connection(ch1.port2);
    const remote1 = (await wrap1.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote1.echo('one'), 'one');

    // Close wrap side of session 1
    wrap1.close();
    ch1.port2.close();

    // Reset host with new endpoint (ch2.port1)
    host.reset(ch2.port1);
    host.expose(service);

    // Wrap from ch2.port2
    const wrap2 = new Connection(ch2.port2);
    const remote2 = (await wrap2.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await remote2.echo('two'), 'two');

    wrap2.close();
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('stale proxy from reset wrap side rejects calls', async () => {
    const {port1, port2} = new MessageChannel();

    const serviceA = {
      identify(): string {
        return 'A';
      },
    };
    const serviceB = {
      identify(): string {
        return 'B';
      },
    };

    // Session 1
    const host = new Connection(port1);
    host.expose(serviceA);
    const wrap = new Connection(port2);
    const staleProxy = (await wrap.waitForReady()) as typeof serviceA;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await staleProxy.identify(), 'A');

    // Reset both sides on the same ports
    host.reset();
    host.expose(serviceB);
    wrap.reset();
    const freshProxy = (await wrap.waitForReady()) as typeof serviceB;

    // Fresh proxy works
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    assert.strictEqual(await freshProxy.identify(), 'B');

    // Stale proxy should throw — it was created in a previous session
    assert.throws(
      () => staleProxy.identify(),
      (err: Error) => {
        assert.match(err.message, /[Ss]tale proxy/);
        return true;
      },
    );

    // Stale property access (via .then) should also reject.
    // The proxy property is a thenable, so we await it to trigger the .then
    // which checks the session.
    try {
      await (staleProxy as unknown as {value: Promise<unknown>}).value;
      assert.fail('Expected stale property access to throw');
    } catch (err) {
      assert.match((err as Error).message, /[Ss]tale proxy/);
    }

    wrap.close();
    host.close();
    port1.close();
    port2.close();
  });

  void test('reused id after peer reset is rejected, not misrouted to the new object', async (t) => {
    const {port1, port2} = new MessageChannel();

    // `a` plays the role of a peer that owns a callback and later resets —
    // e.g. a webview reloading while the extension host still holds a proxy
    // to its old event callback. `b` is that other side, retaining a proxy
    // across the reset.
    const callsToCb2: Array<string> = [];
    const cb1 = (_tag: string): void => {
      // Reaching cb1 at all is also wrong post-reset (it's been replaced),
      // but the call under test targets cb2's reused id, not cb1's.
    };
    const cb2 = (tag: string): void => {
      callsToCb2.push(tag);
    };

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose(cb1);
    const staleProxy = (await b.waitForReady()) as (
      tag: string,
    ) => Promise<unknown>;

    // `a` resets and re-exposes a new callback, which — since ids restart
    // from the same sequence — reclaims cb1's old id.
    a.reset();
    a.expose(cb2);

    // Invoke the stale proxy synchronously, before `b` has processed `a`'s
    // new handshake (that only happens once the event loop turns, and nothing
    // here awaits). The outgoing call is therefore stamped with the session
    // `b` still believes `a` is in — the one cb1 was registered under — so
    // `a` must recognize the mismatch against its actual (post-reset)
    // session and reject rather than routing the call into cb2.
    await assert.rejects(staleProxy('stale'), (err: Error) => {
      assert.match(err.message, /[Ss]tale session/);
      return true;
    });

    assert.deepStrictEqual(
      callsToCb2,
      [],
      'the stale call must not reach the new callback',
    );
  });

  void test('a retained NESTED proxy the peer never re-sent is still rejected after the peer re-handshakes', async (t) => {
    const {port1, port2} = new MessageChannel();

    // The case the session tag exists for, and the one a root does NOT
    // cover: a callback handed out as a call result lives at an id that
    // means one particular object, so a reset() that reclaims the id must
    // not let a retained proxy for the old object route into the new one.
    // Unlike the root (revalidated on every re-handshake — see the retained
    // root test below), `a` never re-sends this id, so its frozen tag stays
    // stale and the call is rejected.
    //
    // Bounded rather than a bare `assert.rejects`: if a regression makes the
    // call hang instead of settling wrong, this fails fast instead of
    // hanging the run.
    const reached: Array<string> = [];

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({
      getCb: (): unknown =>
        proxy((tag: string): void => {
          reached.push(`cb1:${tag}`);
        }),
    });
    const root = (await b.waitForReady()) as {
      getCb(): Promise<(tag: string) => Promise<unknown>>;
    };
    const nested = await root.getCb();
    await nested('first');
    assert.deepStrictEqual(reached, ['cb1:first']);

    // `a` resets and re-exposes; ids restart, so cb2 would reclaim the id
    // `nested` names as soon as it is handed out again.
    a.reset();
    a.expose({
      getCb: (): unknown =>
        proxy((tag: string): void => {
          reached.push(`cb2:${tag}`);
        }),
    });

    // Let `b` process `a`'s new handshake. That revalidates the root's id
    // and nothing else — `nested`'s id is not re-sent, so its tag stays
    // frozen at the previous session.
    await new Promise((r) => setTimeout(r, 50));

    let settled: {ok: true} | {ok: false; error: Error} | undefined;
    const callPromise = nested('stale').then(
      () => {
        settled = {ok: true};
      },
      (error: unknown) => {
        settled = {ok: false, error: error as Error};
      },
    );
    await Promise.race([callPromise, new Promise((r) => setTimeout(r, 1000))]);

    assert.ok(settled !== undefined, 'the stale call must settle, not hang');
    assert.strictEqual(
      settled.ok,
      false,
      'the stale call must reject, not resolve into the new callback',
    );
    assert.match(
      (settled as {ok: false; error: Error}).error.message,
      /[Ss]tale session/,
    );
    assert.deepStrictEqual(
      reached,
      ['cb1:first'],
      'the stale call must not reach the new callback',
    );
  });

  void test('a detached property callable retains its proxy across GC, so its calls stay session-tagged', async (t) => {
    // `const fn = obj.method` then dropping `obj`: the callable sends by raw
    // id, so without anchoring the proxy GC collects it, the finalizer drops
    // the #remoteProxyById entry, and later calls go out with no session tag
    // at all — skipping the peer's staleness check where a live proxy would
    // have been rejected. Observable here as the peer forgetting the object
    // outright, since the finalizer also posts `release`.
    const {setFlagsFromString} = await import('node:v8');
    const {runInNewContext} = await import('node:vm');
    setFlagsFromString('--expose-gc');
    const gc = runInNewContext('gc') as () => void;

    const {port1, port2} = new MessageChannel();
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({
      getObj: (): unknown => proxy({greet: (): string => 'ok'}),
    });
    const root = (await b.waitForReady()) as {
      getObj(): Promise<{greet: () => Promise<string>}>;
    };

    // Read the callable off the proxy WITHOUT binding — binding would retain
    // `obj` by itself and defeat the test.
    let obj: {greet: () => Promise<string>} | undefined = await root.getObj();
    const fn = obj.greet;
    assert.strictEqual(await fn(), 'ok');

    // Drop every other reference to the nested proxy and let GC (and any
    // FinalizationRegistry macrotasks) run.
    obj = undefined;
    for (let i = 0; i < 5; i++) {
      gc();
      await new Promise((r) => setImmediate(r));
    }

    assert.strictEqual(
      await fn(),
      'ok',
      'the detached callable must keep its proxy — and its session entry — alive',
    );
  });

  void test('a retained root is revalidated when the peer re-handshakes, without a second waitForReady()', async (t) => {
    const {port1, port2} = new MessageChannel();

    // A root names "the peer's root service" — a stable role, not one
    // particular object — so a consumer that holds the root across a peer
    // reset keeps working, as it did before session tags existed. The
    // re-handshake is what revalidates it: `b` never calls waitForReady()
    // again, so nothing settles and only the explicit revalidation in the
    // HANDSHAKE_ID branch refreshes the retained root's tag. Without it every
    // later call through the retained root is rejected as stale forever,
    // permanently bricking the connection.
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({greet: (): string => 'v1'});
    const root = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await root.greet(), 'v1');

    a.reset();
    a.expose({greet: (): string => 'v2'});

    // No second waitForReady() — just let the handshake be processed.
    await new Promise((r) => setTimeout(r, 50));

    assert.strictEqual(
      await root.greet(),
      'v2',
      'the retained root must route to the re-exposed root service',
    );
  });

  void test('fresh proxy after peer reset still works (session tag does not over-reject)', async (t) => {
    const {port1, port2} = new MessageChannel();

    const calls: Array<string> = [];
    const cb1 = (tag: string): void => {
      calls.push(`cb1:${tag}`);
    };
    const cb2 = (tag: string): void => {
      calls.push(`cb2:${tag}`);
    };

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose(cb1);
    const firstProxy = (await b.waitForReady()) as (
      tag: string,
    ) => Promise<unknown>;
    await firstProxy('one');
    assert.deepStrictEqual(calls, ['cb1:one']);

    a.reset();
    a.expose(cb2);
    // `b` resets and re-handshakes too (as a well-behaved peer would),
    // getting a genuinely new proxy rather than the cached one for cb1's
    // id. That proxy must work normally — the session tag is meant to
    // catch stale calls, not every call made after any reset.
    b.reset();
    const freshProxy = (await b.waitForReady()) as (
      tag: string,
    ) => Promise<unknown>;

    await freshProxy('two');
    assert.deepStrictEqual(calls, ['cb1:one', 'cb2:two']);
  });

  void test('root proxy after each of two resets carries the correct peer session', async (t) => {
    const {port1, port2} = new MessageChannel();

    // A root re-obtained after each of a series of resets must keep working.
    // This exercises the handshake ordering: #peerSession is recorded before
    // the handshake value is deserialized into the root proxy, which is what
    // a peer predating the `s` wire field falls back to. Get it wrong and
    // every call through a freshly re-obtained root is rejected as stale.
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({greet: (): string => 'v1'});
    let remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await remote.greet(), 'v1');

    a.reset();
    a.expose({greet: (): string => 'v2'});
    b.reset();
    remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await remote.greet(), 'v2');

    a.reset();
    a.expose({greet: (): string => 'v3'});
    b.reset();
    remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await remote.greet(), 'v3');
  });

  void test('stale one-way notify to a reused id is dropped, not misrouted', async (t) => {
    const {port1, port2} = new MessageChannel();

    const calls: Array<string> = [];
    const cb1 = (tag: string): void => {
      calls.push(`cb1:${tag}`);
    };
    const cb2 = (tag: string): void => {
      calls.push(`cb2:${tag}`);
    };

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose(cb1);
    const staleProxy = (await b.waitForReady()) as (
      tag: string,
    ) => Promise<unknown>;

    a.reset();
    a.expose(cb2);

    // A notify has no reply to await, so send it and then round-trip a
    // normal call through a fresh proxy — messages on one port are
    // delivered and processed in order, so by the time the round-trip
    // resolves, the stale notify has already been handled one way or
    // the other.
    notify(staleProxy)('stale');
    // `b` resets too, to get a genuinely new proxy rather than the cached
    // one for cb1's id (see the previous test for why that distinction
    // doesn't otherwise change the outcome).
    b.reset();
    const freshProxy = (await b.waitForReady()) as (
      tag: string,
    ) => Promise<unknown>;
    await freshProxy('fresh');

    assert.deepStrictEqual(
      calls,
      ['cb2:fresh'],
      'the stale notify must not reach cb2 (or cb1)',
    );
  });

  void test('peer reset + re-handshake without resetting this side: the new root still works', async (t) => {
    const {port1, port2} = new MessageChannel();

    // The asymmetric case: only `a` resets (a webview reloading while the
    // extension host keeps its Connection), so `b` re-awaits readiness
    // without a reset of its own. `b`'s proxy cache is therefore intact and
    // the new root resolves to the existing entry for id 0, which — being
    // the root role rather than a reclaimed id — is re-keyed to the new owner
    // session rather than superseded. Get that wrong and every call through
    // the freshly obtained root is rejected as stale.
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({greet: (): string => 'v1'});
    let remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await remote.greet(), 'v1');

    a.reset();
    a.expose({greet: (): string => 'v2'});

    remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(
      await remote.greet(),
      'v2',
      'the re-handshaked root must not be treated as stale',
    );
  });

  void test('release for a superseded session is ignored; an untagged release still releases', async (t) => {
    const {port1, port2} = new MessageChannel();

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({greet: (): string => 'hi'});
    const remote = (await b.waitForReady()) as {greet(): Promise<string>};
    assert.strictEqual(await remote.greet(), 'hi');

    // Forge the frames a peer's finalizer would post, rather than waiting on
    // GC. A release tagged with a session `a` is not in names an id that has
    // since been reused, so honoring it would unregister the live root.
    port2.postMessage({type: 'release', id: 0, session: a._session + 1});
    await new Promise((r) => setTimeout(r, 10));
    assert.strictEqual(
      await remote.greet(),
      'hi',
      'a stale release must not unregister the live target',
    );

    // No tag at all means a peer that predates the field — release
    // unconditionally, as before.
    port2.postMessage({type: 'release', id: 0});
    await new Promise((r) => setTimeout(r, 10));
    await assert.rejects(remote.greet(), (err: Error) => {
      assert.match(err.message, /not found/);
      return true;
    });
  });

  void test('a callback the wrap side owns is protected too, though it never calls expose()', async (t) => {
    const {port1, port2} = new MessageChannel();

    // Tagging must not depend on who called expose(). The wrap side never
    // exposes, so it never sends a handshake of its own — before owner
    // sessions rode on the wire proxies themselves, every host->wrap call
    // went out untagged and a callback proxy retained across the wrap side's
    // reset() silently reached whatever reclaimed its id.
    const calls: Array<string> = [];
    let held: ((tag: string) => Promise<unknown>) | undefined;
    const service = {
      register: (cb: (tag: string) => Promise<unknown>): boolean => {
        held = cb;
        return true;
      },
    };

    const host = new Connection(port1);
    const wrap = new Connection(port2);
    t.after(() => {
      host.close();
      wrap.close();
      port1.close();
      port2.close();
    });

    // Typed with the proxy's async return, not `typeof service` — the remote
    // method is thenable at runtime even though the local one isn't.
    interface Remote {
      register(cb: (tag: string) => Promise<unknown>): Promise<boolean>;
    }

    host.expose(service);
    let remote = (await wrap.waitForReady()) as Remote;
    await remote.register((tag: string): Promise<unknown> => {
      calls.push(`old:${tag}`);
      return Promise.resolve();
    });
    const staleProxy = held;
    assert.ok(staleProxy !== undefined);
    await staleProxy('first');

    // The wrap side resets and registers a new callback, which reclaims the
    // old one's id. The host re-exposes only so the wrap side has a live
    // root to call through.
    wrap.reset();
    host.expose(service);
    remote = (await wrap.waitForReady()) as Remote;
    await remote.register((tag: string): Promise<unknown> => {
      calls.push(`new:${tag}`);
      return Promise.resolve();
    });

    assert.notStrictEqual(
      held,
      staleProxy,
      'the reclaimed id must mint a distinct proxy, not reuse the retained one',
    );

    await assert.rejects(staleProxy('stale'), (err: Error) => {
      assert.match(err.message, /[Ss]tale session/);
      return true;
    });

    // The freshly minted proxy for the same id must still work.
    assert.ok(held !== undefined);
    await held('fresh');

    assert.deepStrictEqual(
      calls,
      ['old:first', 'new:fresh'],
      'the stale call must not reach the new callback',
    );
  });

  void test('a non-root id re-sent after a peer reset mints a new proxy; the retained one stays stale', async (t) => {
    const {port1, port2} = new MessageChannel();

    // Identity is per (owner session, id), so an id re-sent under a new owner
    // session mints a distinct proxy rather than changing what the retained
    // one means: the new proxy works, the old stays frozen at its own owner
    // session and keeps being rejected.
    const calls: Array<string> = [];

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    a.expose({
      getCb: (): unknown =>
        proxy((tag: string): void => {
          calls.push(`old:${tag}`);
        }),
    });
    const root = (await b.waitForReady()) as {
      getCb(): Promise<(tag: string) => Promise<unknown>>;
    };
    const oldProxy = await root.getCb();
    await oldProxy('first');

    a.reset();
    a.expose({
      getCb: (): unknown =>
        proxy((tag: string): void => {
          calls.push(`new:${tag}`);
        }),
    });
    await new Promise((r) => setTimeout(r, 50));

    // The peer re-sends the same id, now naming a different object.
    const newProxy = await root.getCb();
    assert.notStrictEqual(
      newProxy,
      oldProxy,
      'a reclaimed id must mint a distinct proxy',
    );

    await assert.rejects(oldProxy('stale'), (err: Error) => {
      assert.match(err.message, /[Ss]tale session/);
      return true;
    });

    await newProxy('fresh');

    assert.deepStrictEqual(
      calls,
      ['old:first', 'new:fresh'],
      'the retained proxy must not reach the new object',
    );
  });

  void test("a stale call's arguments cannot displace a live proxy for the same id", async (t) => {
    // A stale call's args are deserialized before the call is rejected (so
    // the resources they carry aren't stranded), which means a stale wire
    // proxy naming a reused id reaches the proxy cache. Keyed by id alone it
    // would evict the live current-session proxy's entry: the next receipt of
    // that id would mint a duplicate instead of returning the same object,
    // and the evicted proxy's release would never be sent. Keying by
    // (owner session, id) lets the two coexist.
    const {setFlagsFromString} = await import('node:v8');
    const {runInNewContext} = await import('node:vm');
    setFlagsFromString('--expose-gc');
    const gc = runInNewContext('gc') as () => void;

    const {port1, port2} = new MessageChannel();
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const releases: Array<{id: number; session: number | undefined}> = [];
    port1.addEventListener('message', (event) => {
      const message = (event as {data?: unknown}).data as
        | {type?: string; id: number; session?: number}
        | undefined;
      if (message?.type === 'release') {
        releases.push({id: message.id, session: message.session});
      }
    });

    const cb = (tag: string): string => `got:${tag}`;
    a.expose({getCb: (): unknown => proxy(cb)});
    const root = (await b.waitForReady()) as {
      getCb(): Promise<(tag: string) => Promise<string>>;
    };

    let live: ((tag: string) => Promise<string>) | undefined =
      await root.getCb();
    assert.strictEqual(await live('one'), 'got:one');

    const id = b._proxyId(live);
    assert.ok(id !== undefined, 'the callback proxy must have an id');

    // Forge a stale call into `b`. Its session tag is not `b`'s, so `b`
    // rejects it — but only after deserializing the argument, which names
    // `id` under a different, older owner session.
    port1.postMessage({
      type: 'call',
      id: 999,
      target: 1,
      action: 'call',
      method: 'anything',
      session: b._session + 1,
      args: [{[WIRE_TYPE]: 'proxy', id, o: false, s: 999999}],
    });
    await new Promise((r) => setTimeout(r, 30));

    let again: ((tag: string) => Promise<string>) | undefined =
      await root.getCb();
    assert.strictEqual(
      again,
      live,
      'the stale argument must not displace the live proxy for the same id',
    );
    assert.strictEqual(await again('two'), 'got:two');

    // Bookkeeping half: with the live entry intact, dropping every reference
    // to it must still produce a correctly tagged release for the owner.
    // Both names point at the same proxy, so both have to go.
    live = undefined;
    again = undefined;
    for (let i = 0; i < 8; i++) {
      gc();
      await new Promise((r) => setImmediate(r));
    }
    await new Promise((r) => setTimeout(r, 30));

    assert.ok(
      releases.some((r) => r.id === id && r.session === a._session),
      `the live proxy's release must reach the owner tagged with its current session, got ${JSON.stringify(releases)}`,
    );
  });

  void test('a retained old-session id-0 proxy is never promoted into the root role', async (t) => {
    // A frame naming id 0 under a superseded session mints an *isolated*
    // proxy rather than re-keying the live root backwards, so several live
    // id-0 entries can coexist. The role belongs to exactly one object —
    // `#root` — so a re-handshake must re-key that object specifically.
    // Picking any live id-0 entry instead promotes the isolated stale proxy
    // and leaves the real root frozen at a superseded session, rejecting
    // every call through it from then on.
    const {port1, port2} = new MessageChannel();
    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    // Read the id `b` assigns to its callback straight off the wire, so the
    // forged frame below can be delivered to it and its argument retained.
    let cbId: number | undefined;
    port1.addEventListener('message', (event) => {
      const message = (event as {data?: unknown}).data as
        | {type?: string; method?: string; args?: Array<{id?: number}>}
        | undefined;
      if (message?.type === 'call' && message.method === 'register') {
        cbId ??= message.args?.[0]?.id;
      }
    });

    interface Service {
      tag(): Promise<string>;
      register(cb: (arg: unknown) => void): Promise<boolean>;
    }
    const makeService = (tag: string): object => ({
      tag: (): string => tag,
      register: (cb: unknown): boolean => {
        void cb;
        return true;
      },
    });

    a.expose(makeService('v1'));
    const firstSession = a._session;
    const root = (await b.waitForReady()) as Service;
    assert.strictEqual(await root.tag(), 'v1');

    let captured: unknown;
    await root.register((arg: unknown) => {
      captured = arg;
    });
    assert.ok(cbId !== undefined, "b's callback must have a wire id");

    const reExpose = async (tag: string): Promise<void> => {
      a.reset();
      a.expose(makeService(tag));
      await new Promise((r) => setTimeout(r, 30));
    };

    await reExpose('v2');

    // Deliver a frame naming id 0 under the ORIGINAL, now superseded session
    // and retain the isolated proxy it mints.
    port1.postMessage({
      type: 'call',
      id: -1,
      target: cbId,
      action: 'call',
      method: undefined,
      session: b._session,
      args: [{[WIRE_TYPE]: 'proxy', id: 0, o: false, s: firstSession}],
    });
    await new Promise((r) => setTimeout(r, 30));
    const stale = captured as (() => Promise<unknown>) | undefined;
    assert.ok(stale !== undefined, 'the stale id-0 proxy must be retained');
    const staleTag = b._proxyOwnerSession(stale);

    // Two more re-handshakes. The second is where a scan would pick the
    // stale entry, since by then it precedes the root in insertion order.
    for (const tag of ['v3', 'v4']) {
      await reExpose(tag);
      assert.strictEqual(
        b._proxyOwnerSession(root),
        a._session,
        `the real root must hold the role after re-exposing ${tag}`,
      );
      assert.strictEqual(
        b._proxyOwnerSession(stale),
        staleTag,
        'the stale proxy must stay frozen at its own owner session',
      );
      assert.strictEqual(
        await root.tag(),
        tag,
        'the retained root must keep working across re-handshakes',
      );
    }

    await assert.rejects(
      (stale as unknown as () => Promise<unknown>)(),
      (err: Error) => {
        assert.match(err.message, /[Ss]tale session/);
        return true;
      },
    );
  });

  void test('reset drops unsent batched calls instead of flushing', async () => {
    const {port1, port2} = new MessageChannel();

    let callCount = 0;
    const service = {
      track(): void {
        callCount++;
      },
    };

    // Enable batching
    const host = new Connection(port1, {batching: true});
    host.expose(service);
    const wrap = new Connection(port2, {batching: true});
    const remote = (await wrap.waitForReady()) as typeof service;

    // Make a call — it goes into the batch queue (microtask flush pending).
    // The proxy returns a Promise at runtime even though the type says void.
    const callPromise = remote.track() as unknown as Promise<void>;

    // Reset synchronously BEFORE the microtask flush runs
    wrap.reset();

    // The call should be rejected
    await assert.rejects(callPromise, (err: Error) => {
      assert.strictEqual(err.message, 'Connection reset');
      return true;
    });

    // Wait for any pending microtasks/messages to settle
    await new Promise((r) => setTimeout(r, 20));

    // The remote side should NOT have received the call
    assert.strictEqual(callCount, 0);

    host.close();
    port1.close();
    port2.close();
  });

  void test('double reset is safe', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {value: 99};

    const host = new Connection(port1);
    host.expose(service);
    const wrap1 = new Connection(port2);
    const remote1 = (await wrap1.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    assert.strictEqual(await remote1.value, 99);

    // Double reset — should not throw or corrupt state
    host.reset();
    host.reset();

    host.expose(service);
    wrap1.reset();
    const remote2 = (await wrap1.waitForReady()) as typeof service;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    assert.strictEqual(await remote2.value, 99);

    wrap1.close();
    host.close();
    port1.close();
    port2.close();
  });
});
