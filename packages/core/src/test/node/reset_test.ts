/**
 * Tests for Connection.reset() — reconnection support.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel, type MessagePort} from 'node:worker_threads';
import {Connection} from '../../lib/connection.js';
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
