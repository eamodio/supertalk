import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {expose, wrap} from '../../index.js';
import type {Endpoint} from '../../index.js';
import {AbortSignalHandler} from '../../handlers/abort-signal.js';

/**
 * Wraps a MessagePort and records postMessage calls.
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

void suite('batching', () => {
  void test('multiple synchronous calls are batched', async () => {
    const {port1, port2} = new MessageChannel();
    const spy = spyEndpoint(port2);

    const service = {
      add(a: number, b: number): number {
        return a + b;
      },
      multiply(a: number, b: number): number {
        return a * b;
      },
    };

    expose(service, port1, {batching: true});
    const remote = await wrap<typeof service>(spy, {batching: true});

    // Reset count after handshake
    const countAfterHandshake = spy.postMessageCount;

    // Fire off multiple calls synchronously — should batch
    const results = await Promise.all([
      remote.add(1, 2),
      remote.multiply(3, 4),
      remote.add(5, 6),
    ]);

    assert.deepStrictEqual(results, [3, 12, 11]);

    // All 3 calls should have been sent in a single postMessage
    assert.strictEqual(
      spy.postMessageCount - countAfterHandshake,
      1,
      'Expected 3 synchronous calls to be batched into 1 postMessage',
    );

    port1.close();
    port2.close();
  });

  void test('single call sends without batch wrapper', async () => {
    const {port1, port2} = new MessageChannel();
    const spy = spyEndpoint(port2);

    const service = {
      echo(value: string): string {
        return value;
      },
    };

    expose(service, port1, {batching: true});
    const remote = await wrap<typeof service>(spy, {batching: true});

    // Clear handshake messages
    spy.messages.length = 0;

    const result = await remote.echo('hello');
    assert.strictEqual(result, 'hello');

    // Single message should NOT be wrapped in a batch
    assert.strictEqual(spy.messages.length, 1);
    const sent = spy.messages[0] as {type: string};
    assert.notStrictEqual(
      sent.type,
      'batch',
      'Single message should not be wrapped in a batch',
    );

    port1.close();
    port2.close();
  });

  void test('batching works with only sender opted in', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      greet(name: string): string {
        return `Hello, ${name}!`;
      },
    };

    // Only wrap side (sender of calls) enables batching
    expose(service, port1);
    const remote = await wrap<typeof service>(port2, {batching: true});

    const results = await Promise.all([
      remote.greet('Alice'),
      remote.greet('Bob'),
    ]);

    assert.deepStrictEqual(results, ['Hello, Alice!', 'Hello, Bob!']);

    port1.close();
    port2.close();
  });

  void test('batching enabled on both sides', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      double(n: number): number {
        return n * 2;
      },
    };

    expose(service, port1, {batching: true});
    const remote = await wrap<typeof service>(port2, {batching: true});

    const results = await Promise.all([
      remote.double(1),
      remote.double(2),
      remote.double(3),
    ]);

    assert.deepStrictEqual(results, [2, 4, 6]);

    port1.close();
    port2.close();
  });

  void test('batching works with AbortSignal handler', async () => {
    const {port1, port2} = new MessageChannel();

    const service = {
      longTask(signal: AbortSignal): Promise<string> {
        return Promise.resolve(signal.aborted ? 'aborted' : 'completed');
      },
    };

    expose(service, port1, {
      batching: true,
      handlers: [new AbortSignalHandler()],
    });
    const remote = await wrap<typeof service>(port2, {
      batching: true,
      handlers: [new AbortSignalHandler()],
    });

    const controller = new AbortController();
    const result = await remote.longTask(controller.signal);
    assert.strictEqual(result, 'completed');

    port1.close();
    port2.close();
  });

  void test('batched set then get round-trips correctly', async () => {
    const {port1, port2} = new MessageChannel();
    const spy = spyEndpoint(port2);

    const service = {
      value: 0,
      set(n: number): void {
        this.value = n;
      },
      get(): number {
        return this.value;
      },
    };

    expose(service, port1, {batching: true});
    const remote = await wrap<typeof service>(spy, {batching: true});

    await remote.set(42);
    const result = await remote.get();
    assert.strictEqual(result, 42);

    port1.close();
    port2.close();
  });

  void test('batching disabled by default (no regressions)', async () => {
    const {port1, port2} = new MessageChannel();
    const spy = spyEndpoint(port2);

    const service = {
      inc(n: number): number {
        return n + 1;
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(spy);

    const countAfterHandshake = spy.postMessageCount;

    // Without batching, each call is a separate postMessage
    const results = await Promise.all([
      remote.inc(1),
      remote.inc(2),
      remote.inc(3),
    ]);

    assert.deepStrictEqual(results, [2, 3, 4]);

    // Each call should be a separate postMessage (3 total)
    assert.strictEqual(
      spy.postMessageCount - countAfterHandshake,
      3,
      'Without batching, each call should be a separate postMessage',
    );

    port1.close();
    port2.close();
  });
});
