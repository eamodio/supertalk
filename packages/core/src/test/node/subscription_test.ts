import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {
  expose,
  wrap,
  subscribe,
  proxy,
  Connection,
  ConnectionClosedError,
} from '../../index.js';
import type {Remote} from '../../index.js';

/**
 * Wait for `ms` — enough for a same-process MessageChannel round trip (or
 * several) to complete.
 */
function tick(ms = 15): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// eslint-disable-next-line @typescript-eslint/no-empty-function
function noop(): void {}

/**
 * A tiny event-emitter service: `onTick(cb)` registers a listener and
 * returns a real unsubscribe function, so we can assert the remote side was
 * actually released (not just that delivery locally stopped).
 */
function makeTickService(): {
  service: {onTick(cb: (n: number) => void): () => void};
  emit: (n: number) => void;
  listenerCount: () => number;
} {
  const cbs = new Set<(n: number) => void>();
  return {
    service: {
      onTick(cb: (n: number) => void): () => void {
        cbs.add(cb);
        return () => {
          cbs.delete(cb);
        };
      },
    },
    emit(n: number): void {
      for (const cb of cbs) cb(n);
    },
    listenerCount(): number {
      return cbs.size;
    },
  };
}

void suite('subscribe()', () => {
  void test('subscribe before ready delivers events once the connection becomes ready', async () => {
    const {port1, port2} = new MessageChannel();
    const {service, emit} = makeTickService();
    const received: Array<number> = [];

    // Wrap side first: subscribe and start waiting for ready before the
    // peer has been exposed at all.
    const wrapConn = new Connection(port2);
    const subscription = subscribe<typeof service>(wrapConn, (remote) =>
      remote.onTick((n: number) => received.push(n)),
    );
    void wrapConn.waitForReady();

    // Now the peer shows up.
    const host = new Connection(port1);
    host.expose(service);

    await subscription.ready;
    emit(1);
    await tick();

    assert.deepStrictEqual(received, [1]);

    subscription.unsubscribe();
    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void test('unsubscribe() stops delivery and releases the remote side', async () => {
    const {port1, port2} = new MessageChannel();
    const {service, emit, listenerCount} = makeTickService();
    const received: Array<number> = [];

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2);
    await wrapConn.waitForReady();

    const subscription = subscribe<typeof service>(wrapConn, (remote) =>
      remote.onTick((n: number) => received.push(n)),
    );
    await subscription.ready;
    assert.strictEqual(listenerCount(), 1);

    emit(1);
    await tick();
    assert.deepStrictEqual(received, [1]);

    subscription.unsubscribe();
    await tick();
    assert.strictEqual(listenerCount(), 0, 'the remote unsubscribe ran');

    emit(2);
    await tick();
    assert.deepStrictEqual(
      received,
      [1],
      'no further delivery after unsubscribe',
    );

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void test('unsubscribe() called before the subscribe lands still releases the remote side', async () => {
    const {port1, port2} = new MessageChannel();
    const {service, listenerCount} = makeTickService();

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2);
    await wrapConn.waitForReady();

    const subscription = subscribe<typeof service>(wrapConn, (remote) =>
      remote.onTick(noop),
    );
    // The onTick() RPC call is in flight (the subscriber already ran and
    // sent it, but the response hasn't arrived yet) — unsubscribe now.
    subscription.unsubscribe();

    await tick(30);

    assert.strictEqual(
      listenerCount(),
      0,
      'the remote-side listener must be released even though it was only just added',
    );

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void test('reconnect re-runs the subscriber and delivery resumes with no application-side resubscription', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const session1 = makeTickService();
    const session2 = makeTickService();
    const received: Array<number> = [];

    const host = new Connection(ch1.port1);
    host.expose(session1.service);
    const wrapConn = new Connection(ch1.port2);
    await wrapConn.waitForReady();

    const subscription = subscribe<typeof session1.service>(
      wrapConn,
      (remote) => remote.onTick((n: number) => received.push(n)),
    );
    await subscription.ready;

    session1.emit(1);
    await tick();
    assert.deepStrictEqual(received, [1]);

    // Reconnect onto a fresh endpoint pair with a fresh service instance.
    host.reset(ch2.port1);
    host.expose(session2.service);
    wrapConn.reset(ch2.port2);
    await wrapConn.waitForReady();

    // Give the re-issued onTick() RPC call time to land on the new session.
    await tick(30);

    assert.strictEqual(
      session1.listenerCount(),
      1,
      "the previous session's unsubscribe must never be invoked, only dropped",
    );

    session2.emit(2);
    await tick();

    assert.deepStrictEqual(
      received,
      [1, 2],
      'the subscriber automatically re-ran against the new session with no app-side resubscribe',
    );

    subscription.unsubscribe();
    host.close();
    wrapConn.close();
    ch1.port1.close();
    ch1.port2.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('a synchronously throwing subscriber logs once and leaves the handle usable', async () => {
    const {port1, port2} = new MessageChannel();
    const service = {
      ping(): string {
        return 'pong';
      },
    };
    const logs: Array<Array<unknown>> = [];

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2, {
      logger: {error: (...args: Array<unknown>) => logs.push(args)},
    });
    await wrapConn.waitForReady();

    const subscription = subscribe<typeof service>(wrapConn, () => {
      throw new Error('subscriber boom');
    });

    await assert.rejects(subscription.ready, /subscriber boom/);

    assert.strictEqual(logs.length, 1);
    assert.strictEqual(logs[0]?.[0], 'Subscription failed');
    assert.strictEqual((logs[0][1] as Error).message, 'subscriber boom');

    assert.strictEqual(subscription.closed, false, 'the handle stays usable');
    assert.doesNotThrow(() => {
      subscription.unsubscribe();
    });

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void suite('ready', () => {
    void test('resolves on a successful subscribe', async () => {
      const {port1, port2} = new MessageChannel();
      const {service} = makeTickService();

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2);
      await wrapConn.waitForReady();

      const subscription = subscribe<typeof service>(wrapConn, (remote) =>
        remote.onTick(noop),
      );

      await assert.doesNotReject(subscription.ready);

      subscription.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });

    void test('rejects on a failing subscribe, and an ignored failure produces no unhandled rejection', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        ping(): string {
          return 'pong';
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2, {
        logger: {error: noop},
      });
      await wrapConn.waitForReady();

      let unhandledRejection = false;
      const onUnhandledRejection = (): void => {
        unhandledRejection = true;
      };
      process.on('unhandledRejection', onUnhandledRejection);

      const subscription = subscribe<typeof service>(wrapConn, () => {
        throw new Error('nope');
      });

      // Give the failure time to settle without ever reading `.ready`.
      await tick(20);

      process.off('unhandledRejection', onUnhandledRejection);
      assert.strictEqual(
        unhandledRejection,
        false,
        'an ignored ready failure must not produce an unhandled rejection',
      );

      // Reading it afterward still surfaces the error.
      await assert.rejects(subscription.ready, /nope/);

      subscription.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });

    void test('rejects (not resolves) when the failure reason is undefined, read after settling', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        ping(): string {
          return 'pong';
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2, {
        logger: {error: noop},
      });
      await wrapConn.waitForReady();

      const subscription = subscribe<typeof service>(wrapConn, () =>
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- deliberately testing an undefined rejection reason
        Promise.reject(undefined),
      );

      // Let the failure settle before `ready` is ever read.
      await tick(20);

      await assert.rejects(subscription.ready, (error: unknown) => {
        assert.strictEqual(error, undefined);
        return true;
      });

      subscription.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });

    void test('rejects (not resolves) when the failure reason is undefined, read before settling', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        ping(): string {
          return 'pong';
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2, {
        logger: {error: noop},
      });
      await wrapConn.waitForReady();

      const subscription = subscribe<typeof service>(wrapConn, () =>
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- deliberately testing an undefined rejection reason
        Promise.reject(undefined),
      );

      // Read `ready` immediately, before the failure has settled.
      await assert.rejects(subscription.ready, (error: unknown) => {
        assert.strictEqual(error, undefined);
        return true;
      });

      subscription.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });

    void test('ready after failure returns the same promise instance on repeated reads', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        ping(): string {
          return 'pong';
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2, {logger: {error: noop}});
      await wrapConn.waitForReady();

      const subscription = subscribe<typeof service>(wrapConn, () => {
        throw new Error('nope');
      });

      await assert.rejects(subscription.ready, /nope/);

      assert.strictEqual(
        subscription.ready,
        subscription.ready,
        'ready must be memoized after settling, not a fresh promise per read',
      );

      subscription.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });
  });

  void test('closed transitions, unsubscribe() is idempotent, and Symbol.dispose works with `using`', async () => {
    const {port1, port2} = new MessageChannel();
    const {service, listenerCount} = makeTickService();

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2);
    await wrapConn.waitForReady();

    {
      using subscription = subscribe<typeof service>(wrapConn, (remote) =>
        remote.onTick(noop),
      );
      assert.strictEqual(subscription.closed, false);
      await subscription.ready;
      assert.strictEqual(listenerCount(), 1);
    }

    await tick();
    assert.strictEqual(
      listenerCount(),
      0,
      '`using` released the remote subscription on scope exit',
    );

    const subscription2 = subscribe<typeof service>(wrapConn, (remote) =>
      remote.onTick(noop),
    );
    await subscription2.ready;

    subscription2.unsubscribe();
    assert.strictEqual(subscription2.closed, true);
    assert.doesNotThrow(() => {
      subscription2.unsubscribe();
    });
    assert.strictEqual(subscription2.closed, true);

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void suite('target forms', () => {
    void test('subscribe(remote, ...) — an already-resolved remote proxy', async () => {
      const {port1, port2} = new MessageChannel();
      const {service, listenerCount} = makeTickService();

      expose(service, port1);
      const remote = await wrap<typeof service>(port2);

      const subscription = subscribe(remote, (r) => r.onTick(noop));
      await subscription.ready;
      assert.strictEqual(listenerCount(), 1);

      subscription.unsubscribe();
      port1.close();
      port2.close();
    });

    void test('subscribe(promiseOfRemote, ...) — a pending wrap() promise', async () => {
      const {port1, port2} = new MessageChannel();
      const {service, listenerCount} = makeTickService();

      expose(service, port1);
      const remotePromise = wrap<typeof service>(port2);

      const subscription = subscribe(remotePromise, (r) => r.onTick(noop));
      await subscription.ready;
      assert.strictEqual(listenerCount(), 1);

      subscription.unsubscribe();
      port1.close();
      port2.close();
    });

    void test('subscribe(nestedProxy, ...) — rejected, since the subscriber would receive the root', async () => {
      const {port1, port2} = new MessageChannel();
      const child = {
        listen(): void {
          // never called
        },
      };
      const service = {child: proxy(child)};

      expose(service, port1);
      const remote = await wrap<typeof service>(port2);
      const nested = await remote.child;

      let subscriberRan = false;
      // The constructor routes the TypeError to `ready` rather than
      // throwing, matching every other target-resolution failure.
      const subscription = subscribe<typeof child>(nested, () => {
        subscriberRan = true;
      });
      await assert.rejects(subscription.ready, (error: unknown) => {
        assert.ok(error instanceof TypeError);
        assert.match(error.message, /root proxy/);
        return true;
      });
      assert.strictEqual(subscriberRan, false);

      port1.close();
      port2.close();
    });

    void test('a stale nested proxy from before a reset() is still rejected; a stale root still works', async () => {
      const {port1, port2} = new MessageChannel();
      const child = {
        listen(): void {
          // never called
        },
      };
      const service = {child: proxy(child)};

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2, {logger: {error: noop}});
      const staleRoot = (await wrapConn.waitForReady()) as Remote<
        typeof service
      >;
      const staleNested = await staleRoot.child;

      // Reset and re-handshake: the registries are cleared, but root-ness
      // must survive so the two stale proxies are still told apart.
      wrapConn.reset();
      host.expose(service);
      await wrapConn.waitForReady();

      const rejected = subscribe<typeof child>(
        staleNested as unknown as Remote<typeof child>,
        () => {
          assert.fail('subscriber must not run for a nested target');
        },
      );
      await assert.rejects(
        rejected.ready,
        /root proxy/,
        'stale nested rejected',
      );

      let received: unknown;
      const accepted = subscribe<typeof service>(
        staleRoot as unknown as Remote<typeof service>,
        (remote) => {
          received = remote;
        },
      );
      await accepted.ready;
      assert.ok(received !== undefined, 'subscriber ran with the new root');

      rejected.unsubscribe();
      accepted.unsubscribe();
      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });
  });

  void suite('ConnectionClosedError (P2b)', () => {
    void test('close() rejects in-flight calls with ConnectionClosedError, reason "closed"', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        neverResolve(): Promise<never> {
          return new Promise(() => {
            // intentionally never resolves
          });
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2);
      const remote = (await wrapConn.waitForReady()) as typeof service;

      const pending = remote.neverResolve();
      await tick();
      wrapConn.close();

      await assert.rejects(pending, (err: unknown) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.ok(err instanceof Error);
        assert.strictEqual(err.name, 'ConnectionClosedError');
        assert.strictEqual(err.reason, 'closed');
        assert.strictEqual(err.message, 'Connection closed');
        return true;
      });

      host.close();
      port1.close();
      port2.close();
    });

    void test('reset() rejects in-flight calls with ConnectionClosedError, reason "reset"', async () => {
      const {port1, port2} = new MessageChannel();
      const service = {
        neverResolve(): Promise<never> {
          return new Promise(() => {
            // intentionally never resolves
          });
        },
      };

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2);
      const remote = (await wrapConn.waitForReady()) as typeof service;

      const pending = remote.neverResolve();
      await tick();
      wrapConn.reset();

      await assert.rejects(pending, (err: unknown) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.ok(err instanceof Error);
        assert.strictEqual(err.name, 'ConnectionClosedError');
        assert.strictEqual(err.reason, 'reset');
        assert.strictEqual(err.message, 'Connection reset');
        return true;
      });

      host.close();
      wrapConn.close();
      port1.close();
      port2.close();
    });

    void test('subscribe() on an already-closed connection: ready rejects with ConnectionClosedError instead of hanging', async () => {
      const {port1, port2} = new MessageChannel();
      const {service} = makeTickService();

      const host = new Connection(port1);
      host.expose(service);
      const wrapConn = new Connection(port2);
      await wrapConn.waitForReady();
      wrapConn.close();

      const subscription = subscribe<typeof service>(wrapConn, (remote) =>
        remote.onTick(noop),
      );

      await assert.rejects(subscription.ready, (err: unknown) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.strictEqual(err.reason, 'closed');
        return true;
      });

      host.close();
      port1.close();
      port2.close();
    });

    void test('close() while a subscription is waiting: ready rejects with ConnectionClosedError, nothing logged', async () => {
      const {port1, port2} = new MessageChannel();
      const {service: _service} = makeTickService();
      const logs: Array<Array<unknown>> = [];

      // No host exposed at all — the handshake never completes, so the
      // subscription is registered (via _onReady) but never issued.
      const wrapConn = new Connection(port2, {
        logger: {error: (...args: Array<unknown>) => logs.push(args)},
      });
      const subscription = subscribe<typeof _service>(wrapConn, (remote) =>
        remote.onTick(noop),
      );
      // Never resolves (no host) — close() below settles it via
      // ConnectionClosedError, so swallow it to avoid an unhandled rejection.
      void wrapConn.waitForReady().catch(() => {
        // expected: rejected by close() below
      });

      wrapConn.close();

      await assert.rejects(subscription.ready, (err: unknown) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.strictEqual(err.reason, 'closed');
        return true;
      });

      assert.strictEqual(
        logs.length,
        0,
        'a deliberate close() must not be logged as a subscription failure',
      );

      port1.close();
      port2.close();
    });
  });

  void test('unsubscribe() between reset() and the next handshake is silent — the stale remote release is skipped', async () => {
    const {port1, port2} = new MessageChannel();
    const {service, listenerCount} = makeTickService();
    const logs: Array<Array<unknown>> = [];

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2, {
      logger: {error: (...args: Array<unknown>) => logs.push(args)},
    });
    await wrapConn.waitForReady();

    const subscription = subscribe<typeof service>(wrapConn, (remote) =>
      remote.onTick(noop),
    );
    await subscription.ready;
    assert.strictEqual(listenerCount(), 1);

    wrapConn.reset();
    // The stored unsubscribe belongs to the dead session — invoking it could
    // only throw 'Stale proxy from previous session', so it must be skipped
    // and nothing logged.
    subscription.unsubscribe();
    await tick();
    assert.strictEqual(subscription.closed, true);
    assert.strictEqual(logs.length, 0, 'no stale-proxy failure logged');

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });
});
