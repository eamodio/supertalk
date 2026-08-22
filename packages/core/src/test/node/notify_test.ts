import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {
  expose,
  wrap,
  notify,
  handle,
  Connection,
  WIRE_TYPE,
} from '../../index.js';
import type {Endpoint, Handle} from '../../index.js';

/**
 * Wraps a MessagePort and records postMessage calls.
 * (Local copy of the helper in batching_test.ts.)
 */
function spyEndpoint(
  port: Endpoint,
): Endpoint & {postMessageCount: number; messages: Array<unknown>} {
  let postMessageCount = 0;
  const messages: Array<unknown> = [];
  return {
    get postMessageCount() {
      return postMessageCount;
    },
    messages,
    postMessage(message: unknown, transfer?: Array<Transferable>) {
      postMessageCount++;
      messages.push(message);
      port.postMessage(message, transfer);
    },
    addEventListener(type: 'message', listener: (event: MessageEvent) => void) {
      port.addEventListener(type, listener);
    },
    removeEventListener(
      type: 'message',
      listener: (event: MessageEvent) => void,
    ) {
      port.removeEventListener(type, listener);
    },
  };
}

void suite('notify', () => {
  void test('produces exactly one message and no response', async () => {
    const {port1, port2} = new MessageChannel();
    const spy1 = spyEndpoint(port1);
    const spy2 = spyEndpoint(port2);

    const service = {
      greet(name: string): string {
        return `Hello, ${name}!`;
      },
    };

    expose(service, spy1);
    const remote = await wrap<typeof service>(spy2);

    spy1.messages.length = 0;
    spy2.messages.length = 0;

    notify(remote).greet('world');

    // Round-trip a normal call so we know the notify was fully processed
    // by the receiver before we assert on it (messages are delivered and
    // processed in order).
    await remote.greet('sync');

    assert.strictEqual(
      spy2.postMessageCount,
      2,
      'the notify plus the follow-up call',
    );
    const notifyMsg = spy2.messages[0] as {type: string; id: number};
    assert.strictEqual(notifyMsg.type, 'call');
    assert.strictEqual(notifyMsg.id, -1);

    assert.ok(
      !spy1.messages.some((m) => (m as {id?: number}).id === -1),
      'the receiver must never post a response for a notify',
    );

    port1.close();
    port2.close();
  });

  void test('the remote method runs with the right arguments', async () => {
    const {port1, port2} = new MessageChannel();
    const calls: Array<[string, number]> = [];

    const service = {
      record(name: string, n: number): void {
        calls.push([name, n]);
      },
      noop(): string {
        return 'ok';
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2);

    notify(remote).record('a', 1);
    await remote.noop();

    assert.deepStrictEqual(calls, [['a', 1]]);

    port1.close();
    port2.close();
  });

  void test('notify() on a function proxy invokes the callback', async () => {
    const {port1, port2} = new MessageChannel();
    const received: Array<number> = [];
    let emit: ((n: number) => void) | undefined;

    const service = {
      subscribe(cb: (n: number) => void): void {
        const notifier = notify(cb);
        emit = (n: number) => notifier(n);
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2);

    await remote.subscribe((n: number) => {
      received.push(n);
    });

    emit?.(42);
    // Give the fire-and-forget message a tick to arrive and be handled.
    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.deepStrictEqual(received, [42]);

    port1.close();
    port2.close();
  });

  void test('a throwing remote method logs on the receiver, posts no throw, and rejects nothing on the sender', async () => {
    const {port1, port2} = new MessageChannel();
    const spy1 = spyEndpoint(port1);
    const receiverLogs: Array<Array<unknown>> = [];

    const service = {
      fail(): void {
        throw new Error('boom');
      },
      ping(): string {
        return 'pong';
      },
    };

    expose(service, spy1, {
      logger: {error: (...args: Array<unknown>) => receiverLogs.push(args)},
    });
    const remote = await wrap<typeof service>(port2);

    spy1.messages.length = 0;

    const returnValue = notify(remote).fail();
    assert.strictEqual(returnValue, undefined);

    await remote.ping();

    assert.strictEqual(receiverLogs.length, 1);
    assert.strictEqual(
      receiverLogs[0]?.[0],
      'Uncaught error in notify handler',
    );

    assert.ok(
      !spy1.messages.some((m) => (m as {id?: number}).id === -1),
      'no throw should be posted for a failing notify',
    );

    port1.close();
    port2.close();
  });

  void test('a serialization failure on the sender logs and does not throw out of the call', async () => {
    const {port1, port2} = new MessageChannel();
    const senderLogs: Array<Array<unknown>> = [];

    const service = {
      echo(_value: unknown): string {
        return 'ok';
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2, {
      debug: true,
      logger: {error: (...args: Array<unknown>) => senderLogs.push(args)},
    });

    assert.doesNotThrow(() => {
      // A nested function argument is only rejected in debug mode when
      // nestedProxies is off — that's exactly the failure this exercises.
      notify(remote).echo({fn: () => 'nested'});
    });

    assert.strictEqual(senderLogs.length, 1);
    assert.strictEqual(senderLogs[0]?.[0], 'Failed to send notify');

    port1.close();
    port2.close();
  });

  void test('notifies batch with regular calls and preserve ordering', async () => {
    const {port1, port2} = new MessageChannel();
    const spy2 = spyEndpoint(port2);

    const order: Array<string> = [];
    const service = {
      a(): void {
        order.push('a');
      },
      b(n: number): number {
        order.push(`b${String(n)}`);
        return n;
      },
    };

    expose(service, port1, {batching: true});
    const remote = await wrap<typeof service>(spy2, {batching: true});

    spy2.messages.length = 0;

    notify(remote).a();
    const pending = remote.b(1);
    notify(remote).a();

    await pending;

    assert.strictEqual(
      spy2.postMessageCount,
      1,
      'all three synchronous sends should batch into one postMessage',
    );
    const sent = spy2.messages[0] as {
      type: string;
      messages?: Array<unknown>;
    };
    assert.strictEqual(sent.type, 'batch');
    assert.strictEqual(sent.messages?.length, 3);

    assert.deepStrictEqual(order, ['a', 'b1', 'a']);

    port1.close();
    port2.close();
  });

  void test('notify() throws TypeError on a non-proxy', () => {
    assert.throws(() => notify({}), TypeError);
    assert.throws(() => notify(42), TypeError);
    assert.throws(() => notify('str'), TypeError);
    assert.throws(() => notify(null), TypeError);
    assert.throws(() => notify(undefined), TypeError);
  });

  void test('notify() throws TypeError on a handle() (no callable surface)', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      getHandle(): Handle<{id: number}> {
        return handle({id: 1});
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2);

    const h = await remote.getHandle();
    assert.throws(() => notify(h), TypeError);

    port1.close();
    port2.close();
  });

  void test('after reset(), a notifier created before the reset throws synchronously', async () => {
    const {port1, port2} = new MessageChannel();
    const service = {
      greet(name: string): string {
        return `Hello, ${name}!`;
      },
    };

    const host = new Connection(port1);
    host.expose(service);
    const wrapConn = new Connection(port2);
    const remote = (await wrapConn.waitForReady()) as typeof service;

    const notifier = notify(remote);
    assert.doesNotThrow(() => {
      notifier.greet('world'); // fine before the reset
    });

    wrapConn.reset();

    assert.throws(() => {
      notifier.greet('world');
    }, /Stale proxy from previous session/);

    host.close();
    wrapConn.close();
    port1.close();
    port2.close();
  });

  void test('interop: a simulated old receiver that answers everything produces no stray rejection or stuck pending call', async () => {
    const {port1, port2} = new MessageChannel();

    // Simulate an old peer: it answers every 'call' message with a return,
    // including id: -1. A new sender must simply drop that stray response
    // since it never registered a pending call for a notify.
    // (Use the Node EventEmitter-style `.on()` API rather than
    // `addEventListener` to sidestep a DOM-vs-Node MessageEvent typing
    // conflict on raw MessagePort.)
    port1.on('message', (value: unknown) => {
      const data = value as {type: string; id: number} | undefined;
      if (data?.type === 'call') {
        port1.postMessage({type: 'return', id: data.id, value: 42});
      }
    });
    port1.start();

    // Hand-craft the handshake reply an old expose() side would send, since
    // there's no real expose() on this simulated old peer.
    port1.postMessage({
      type: 'return',
      id: 0,
      value: {[WIRE_TYPE]: 'proxy', id: 0, o: false},
    });

    const remote = await wrap<{greet: (name: string) => string}>(port2);

    let unhandledRejection = false;
    const onUnhandledRejection = (): void => {
      unhandledRejection = true;
    };
    process.on('unhandledRejection', onUnhandledRejection);
    try {
      notify(remote).greet('world');
      // Give the stray return time to arrive and be (correctly) dropped.
      await new Promise((resolve) => setTimeout(resolve, 20));
    } finally {
      process.off('unhandledRejection', onUnhandledRejection);
    }

    assert.strictEqual(
      unhandledRejection,
      false,
      'a stray return for id -1 must not produce an unhandled rejection',
    );

    // The connection must still work normally afterward — no leftover
    // pending-call state from the notify.
    const result = await remote.greet('again');
    assert.strictEqual(result, 42);

    port1.close();
    port2.close();
  });
});
