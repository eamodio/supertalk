/**
 * Tests for Connection.callerSession — the sender session of the call
 * currently being dispatched by #handleCall.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {Connection} from '../../lib/connection.js';
import {setupPair} from './test-utils.js';

void suite('Connection.callerSession', () => {
  void test('is visible inside a handler and matches the callers session', async (t) => {
    const {port1, port2} = new MessageChannel();

    let seen: number | undefined;
    const service = {
      probe(): number | undefined {
        seen = host.callerSession;
        return seen;
      },
    };

    const {host, remote, wrap} = await setupPair(service, port1, port2);
    t.after(() => {
      wrap.close();
      host.close();
      port1.close();
      port2.close();
    });

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    const returned = await remote.probe();
    assert.strictEqual(seen, wrap._session);
    assert.strictEqual(returned, wrap._session);
  });

  void test('distinguishes two distinct client connections into the same exposed object', async (t) => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    interface Snapshot {
      fromA: number | undefined;
      fromB: number | undefined;
    }
    let seenDuringA: Snapshot | undefined;
    let seenDuringB: Snapshot | undefined;
    const service = {
      probe(): void {
        // Only the host whose #handleCall is currently dispatching this
        // call has a defined callerSession; the other host is idle.
        const snapshot: Snapshot = {
          fromA: hostA.callerSession,
          fromB: hostB.callerSession,
        };
        if (seenDuringA === undefined) {
          seenDuringA = snapshot;

          return;
        }
        seenDuringB = snapshot;
      },
    };

    const hostA = new Connection(ch1.port1);
    hostA.expose(service);
    const wrapA = new Connection(ch1.port2);
    const remoteA = (await wrapA.waitForReady()) as typeof service;

    const hostB = new Connection(ch2.port1);
    hostB.expose(service);
    const wrapB = new Connection(ch2.port2);
    const remoteB = (await wrapB.waitForReady()) as typeof service;

    t.after(() => {
      wrapA.close();
      wrapB.close();
      hostA.close();
      hostB.close();
      ch1.port1.close();
      ch1.port2.close();
      ch2.port1.close();
      ch2.port2.close();
    });

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    await remoteA.probe();
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    await remoteB.probe();

    assert.ok(seenDuringA !== undefined, 'probe() via A must have run');
    assert.ok(seenDuringB !== undefined, 'probe() via B must have run');
    assert.strictEqual(seenDuringA.fromA, wrapA._session);
    assert.strictEqual(seenDuringA.fromB, undefined);
    assert.strictEqual(seenDuringB.fromB, wrapB._session);
    assert.strictEqual(seenDuringB.fromA, undefined);
    assert.notStrictEqual(wrapA._session, wrapB._session);
  });

  void test('is undefined on the host outside any dispatch', async (t) => {
    const {port1, port2} = new MessageChannel();

    const service = {
      noop(): void {
        // intentionally empty
      },
    };

    const {host, remote, wrap} = await setupPair(service, port1, port2);
    t.after(() => {
      wrap.close();
      host.close();
      port1.close();
      port2.close();
    });

    assert.strictEqual(host.callerSession, undefined);

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    await remote.noop();

    assert.strictEqual(host.callerSession, undefined);
  });

  void test('a remounted peer reports a different callerSession than its predecessor', async (t) => {
    const {port1, port2} = new MessageChannel();

    const seenByCall: Array<number | undefined> = [];
    const service = {
      probe(): void {
        seenByCall.push(host.callerSession);
      },
    };

    const host = new Connection(port1);
    host.expose(service);
    t.after(() => {
      host.close();
      port1.close();
      port2.close();
    });

    const wrap = new Connection(port2);
    let remote = (await wrap.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    await remote.probe();
    const firstSession = wrap._session;
    assert.strictEqual(seenByCall[0], firstSession);

    // The peer (wrap side) resets — its own session id is regenerated — and
    // reconnects to the same host over the same port.
    wrap.reset();
    // Register the wait BEFORE the host re-announces: only expose() sends the
    // handshake frame, and nothing re-sends it automatically the way RpcHost
    // does in response to a client announcement.
    const reconnected = wrap.waitForReady();
    host.expose(service);
    remote = (await reconnected) as typeof service;
    t.after(() => wrap.close());

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
    await remote.probe();
    const secondSession = wrap._session;

    assert.strictEqual(seenByCall[1], secondSession);
    assert.notStrictEqual(
      secondSession,
      firstSession,
      'reset() must regenerate the session id',
    );
    assert.notStrictEqual(
      seenByCall[1],
      seenByCall[0],
      'a remounted client must report a different callerSession than its predecessor',
    );
  });

  void test('re-entrant dispatch never leaks one caller into another', async (t) => {
    // `a` exposes a root whose `outer()` method calls back into `remoteA`
    // (its own root, reached through `b`'s connection) before returning.
    // That second call is dispatched by `a.#handleCall` a second time while
    // the first dispatch is still suspended awaiting it — genuine
    // re-entrancy on the SAME Connection's #handleCall.
    //
    // The context window is scoped to each dispatch's SYNCHRONOUS extent, so
    // `outer()` sees its caller at entry and `undefined` once it resumes past
    // its own await — reads after an await are outside the documented
    // contract. What must hold is that no dispatch ever observes ANOTHER
    // dispatch's caller, and that nothing is left set once both settle.
    const {port1, port2} = new MessageChannel();

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const before: Array<number | undefined> = [];
    const after: Array<number | undefined> = [];

    interface RootA {
      outer(): Promise<void>;
      inner(): Promise<void>;
    }

    const rootA: RootA = {
      outer: async (): Promise<void> => {
        before.push(a.callerSession);
        await remoteA.inner();
        after.push(a.callerSession);
      },
      inner: async (): Promise<void> => {
        // Nested dispatch — reachable is all that matters here.
      },
    };

    a.expose(rootA);
    const remoteA = (await b.waitForReady()) as RootA;

    const outerCall = remoteA.outer();
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      outerCall,
      new Promise((r) => {
        timer = setTimeout(r, 1000);
      }),
    ]);
    clearTimeout(timer);

    assert.strictEqual(before.length, 1, 'outer() must have run');
    assert.strictEqual(
      after.length,
      1,
      'outer() must have resumed after the nested call settled',
    );
    assert.strictEqual(
      after[0],
      undefined,
      'a read after an await is outside the contract: the window closed when outer() suspended',
    );
    assert.strictEqual(
      a.callerSession,
      undefined,
      'no dispatch may leave callerSession set once it settles',
    );
    assert.strictEqual(
      before[0],
      b._session,
      'callerSession before the nested call must be the outer caller',
    );
  });

  void test('batched calls each observe the correct callerSession synchronously', async (t) => {
    // With batching enabled, two calls issued in the same turn go out in one
    // BatchMessage and #onMessage dispatches them back-to-back synchronously
    // (see #withCallerSession's doc: batched same-turn dispatches are exactly
    // what its per-dispatch window scoping exists to keep separate). Assert
    // neither call's window stomps the other's.
    const {port1, port2} = new MessageChannel();

    const seen: Array<number | undefined> = [];
    const service = {
      first(): void {
        seen.push(host.callerSession);
      },
      second(): void {
        seen.push(host.callerSession);
      },
    };

    const host = new Connection(port1, {batching: true});
    const wrap = new Connection(port2, {batching: true});
    host.expose(service);
    const remote = (await wrap.waitForReady()) as typeof service;
    t.after(() => {
      wrap.close();
      host.close();
      port1.close();
      port2.close();
    });

    const received: Array<unknown> = [];
    port1.addEventListener('message', (event) => {
      received.push((event as {data?: unknown}).data);
    });

    // Issued synchronously in the same turn, so batching queues both into a
    // single BatchMessage.
    await Promise.all([
      // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
      remote.first(),
      // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy method is thenable at runtime
      remote.second(),
    ]);

    const batches = received.filter(
      (m): m is {type: string; messages: Array<unknown>} =>
        (m as {type?: string}).type === 'batch',
    );
    assert.strictEqual(
      batches.length,
      1,
      'the two synchronous calls must be delivered in a single BatchMessage',
    );
    assert.strictEqual(batches[0]?.messages.length, 2);

    assert.strictEqual(seen.length, 2, 'both handlers must have run');
    assert.strictEqual(seen[0], wrap._session);
    assert.strictEqual(seen[1], wrap._session);
  });

  void test('a returned-but-unawaited remote property attributes the resulting local get to the peer (top-level property marker in #fromWire)', async (t) => {
    // B's `forward` getter returns `aProxy.someProp` WITHOUT awaiting it —
    // that lazy ProxyProperty (see #createProxyProperty) is what B's
    // #toWire serializes as a top-level 'property' wire marker rather than
    // resolving it to a value first. A receives that marker in its return
    // frame and #fromWire runs the local `someProp` get inside
    // #withCallerSession(this.#peerSession, ...) — this is what's under
    // test: that the get is attributed to the peer, same as an ordinary
    // direct call from it.
    const {port1, port2} = new MessageChannel();

    const a = new Connection(port1);
    const b = new Connection(port2);
    t.after(() => {
      a.close();
      b.close();
      port1.close();
      port2.close();
    });

    const SENTINEL = 'sentinel-value';
    let seenViaProperty: number | undefined;
    let seenViaDirectCall: number | undefined;

    interface RemoteA {
      someProp: Promise<string>;
      direct(): Promise<void>;
    }

    const serviceA = {
      get someProp(): string {
        seenViaProperty = a.callerSession;
        return SENTINEL;
      },
      direct(): void {
        seenViaDirectCall = a.callerSession;
      },
    };

    a.expose(serviceA);
    const aProxy = (await b.waitForReady()) as RemoteA;

    interface RemoteB {
      forward: Promise<string>;
    }

    const serviceB = {
      // NOT awaited: `aProxy.someProp` stays a lazy ProxyProperty, so B's
      // #toWire serializes this `get`'s result as a top-level 'property'
      // wire marker instead of resolving it to a plain value first.
      get forward(): unknown {
        return aProxy.someProp;
      },
    };

    b.expose(serviceB);
    const bProxy = (await a.waitForReady()) as RemoteB;

    // Baseline: what A observes for an ordinary direct call from B, to
    // compare the property-marker-attributed session against.
    await aProxy.direct();
    assert.ok(
      seenViaDirectCall !== undefined,
      'the baseline direct call must have run',
    );

    const forwarded = await bProxy.forward;

    assert.strictEqual(forwarded, SENTINEL);
    assert.ok(
      seenViaProperty !== undefined,
      "the property get reached via #fromWire's property marker must see a defined callerSession",
    );
    assert.strictEqual(
      seenViaProperty,
      seenViaDirectCall,
      'the property-marker-attributed session must match an ordinary direct call from the same peer',
    );
  });
});
