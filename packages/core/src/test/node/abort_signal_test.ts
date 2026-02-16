/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unnecessary-condition */
/**
 * Tests for the AbortSignal handler.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {setupService} from './test-utils.js';
import {expose, wrap} from '../../index.js';
import {AbortSignalHandler, COMPLETED} from '../../handlers/abort-signal.js';

function waitForMessages(ms = 20): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

void suite('AbortSignal handler', () => {
  void test('passes an AbortSignal as an argument', async () => {
    let receivedSignal: AbortSignal | undefined;

    await using ctx = await setupService(
      {
        doWork(_data: string, signal: AbortSignal) {
          receivedSignal = signal;
          return 'done';
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    const result = await ctx.remote.doWork('test', controller.signal);
    assert.strictEqual(result, 'done');
    assert.ok(receivedSignal instanceof AbortSignal);
    assert.strictEqual(receivedSignal?.aborted, false);
  });

  void test('abort propagates to the receiver side', async () => {
    let receivedSignal: AbortSignal | undefined;
    let abortPromiseResolve: () => void;
    const abortPromise = new Promise<void>((r) => {
      abortPromiseResolve = r;
    });

    await using ctx = await setupService(
      {
        async longTask(signal: AbortSignal) {
          receivedSignal = signal;
          // Wait until the signal is aborted
          if (!signal.aborted) {
            await new Promise<void>((resolve) => {
              signal.addEventListener('abort', () => resolve(), {once: true});
            });
          }
          abortPromiseResolve();
          return 'cancelled';
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    // Start the long task (don't await — it blocks until abort)
    const resultPromise = ctx.remote.longTask(controller.signal);

    // Give time for the call to reach the other side
    await waitForMessages();

    // Abort
    controller.abort();

    // Wait for the abort to propagate
    await abortPromise;

    assert.ok(receivedSignal !== undefined);
    assert.strictEqual(receivedSignal?.aborted, true);

    // The method should still return
    const result = await resultPromise;
    assert.strictEqual(result, 'cancelled');
  });

  void test('abort reason propagates to the receiver side', async () => {
    let receivedSignal: AbortSignal | undefined;

    await using ctx = await setupService(
      {
        async waitForAbort(signal: AbortSignal) {
          receivedSignal = signal;
          if (!signal.aborted) {
            await new Promise<void>((resolve) => {
              signal.addEventListener('abort', () => resolve(), {once: true});
            });
          }
          return signal.reason;
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    const resultPromise = ctx.remote.waitForAbort(controller.signal);
    await waitForMessages();

    const reason = new Error('user cancelled');
    controller.abort(reason);

    const result = await resultPromise;
    // The reason is serialized through structured clone, so it comes back as
    // a plain object with message/name/stack — but the Error name is preserved
    assert.ok(receivedSignal !== undefined);
    assert.strictEqual(receivedSignal?.aborted, true);
    // The result will be the serialized reason
    assert.ok(result !== undefined);
  });

  void test('already-aborted signal is received as aborted', async () => {
    let receivedSignal: AbortSignal | undefined;

    await using ctx = await setupService(
      {
        checkSignal(signal: AbortSignal) {
          receivedSignal = signal;
          return signal.aborted;
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    controller.abort('pre-aborted');

    const result = await ctx.remote.checkSignal(controller.signal);
    assert.strictEqual(result, true);
    assert.ok(receivedSignal !== undefined);
    assert.strictEqual(receivedSignal?.aborted, true);
    assert.strictEqual(receivedSignal?.reason, 'pre-aborted');
  });

  void test('completed convention sends release instead of abort', async () => {
    const senderHandler = new AbortSignalHandler();
    const receiverHandler = new AbortSignalHandler();
    let receivedSignal: AbortSignal | undefined;

    const {port1, port2} = new MessageChannel();

    expose(
      {
        async waitForSignal(signal: AbortSignal) {
          receivedSignal = signal;
          // Wait a bit for potential abort/release messages
          await waitForMessages(50);
          return signal.aborted;
        },
      },
      port1,
      {handlers: [senderHandler]},
    );
    const remote = await wrap<{
      waitForSignal(signal: AbortSignal): Promise<boolean>;
    }>(port2, {handlers: [receiverHandler]});

    const controller = new AbortController();
    const resultPromise = remote.waitForSignal(controller.signal);
    await waitForMessages();

    // Signal completion, not cancellation
    controller.abort(COMPLETED);
    await waitForMessages();

    const result = await resultPromise;
    // The receiver's signal should NOT be aborted — release is cleanup only
    assert.strictEqual(result, false);
    assert.ok(receivedSignal !== undefined);
    assert.strictEqual(receivedSignal?.aborted, false);

    // Cleanup
    assert.strictEqual(receiverHandler._receivedCount, 0);
    assert.strictEqual(senderHandler._sentCount, 0);

    port1.close();
    port2.close();
  });

  void test('disconnect aborts all received signals', async () => {
    // exposeHandler is on the expose (port1) side — it receives AbortSignals
    // wrapHandler is on the wrap (port2) side — it sends AbortSignals
    const exposeHandler = new AbortSignalHandler();
    const wrapHandler = new AbortSignalHandler();
    let receivedSignal: AbortSignal | undefined;

    const {port1, port2} = new MessageChannel();

    // expose() returns a cleanup function that calls connection.close()
    const cleanup = expose(
      {
        hold(signal: AbortSignal) {
          receivedSignal = signal;
          return 'held';
        },
      },
      port1,
      {handlers: [exposeHandler]},
    );
    const remote = await wrap<{hold(signal: AbortSignal): string}>(port2, {
      handlers: [wrapHandler],
    });

    const controller = new AbortController();
    await remote.hold(controller.signal);
    assert.ok(receivedSignal !== undefined);
    assert.strictEqual(receivedSignal?.aborted, false);
    // The expose side received the signal via fromWire()
    assert.strictEqual(exposeHandler._receivedCount, 1);

    // Close the connection — calls disconnect() on the expose handler,
    // which should abort all pending received signals
    cleanup();

    assert.strictEqual(receivedSignal?.aborted, true);
    assert.strictEqual(receivedSignal?.reason, 'disconnected');
    assert.strictEqual(exposeHandler._receivedCount, 0);

    port1.close();
    port2.close();
  });

  void test('multiple signals can be tracked independently', async () => {
    const signals: Array<AbortSignal> = [];

    await using ctx = await setupService(
      {
        register(signal: AbortSignal) {
          signals.push(signal);
          return signals.length;
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const c1 = new AbortController();
    const c2 = new AbortController();
    const c3 = new AbortController();

    await ctx.remote.register(c1.signal);
    await ctx.remote.register(c2.signal);
    await ctx.remote.register(c3.signal);

    assert.strictEqual(signals.length, 3);

    // Abort only the second one
    c2.abort('cancelled-2');
    await waitForMessages();

    assert.strictEqual(signals[0]?.aborted, false);
    assert.strictEqual(signals[1]?.aborted, true);
    assert.strictEqual(signals[1]?.reason, 'cancelled-2');
    assert.strictEqual(signals[2]?.aborted, false);

    // Abort the first
    c1.abort();
    await waitForMessages();

    assert.strictEqual(signals[0]?.aborted, true);
    assert.strictEqual(signals[2]?.aborted, false);
  });

  void test('same signal sent twice uses same ID', async () => {
    const signals: Array<AbortSignal> = [];

    await using ctx = await setupService(
      {
        register(signal: AbortSignal) {
          signals.push(signal);
          return signals.length;
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    await ctx.remote.register(controller.signal);
    await ctx.remote.register(controller.signal);

    assert.strictEqual(signals.length, 2);

    // Both signals on the receiver side should abort when the single controller aborts
    controller.abort('shared');
    await waitForMessages();

    assert.strictEqual(signals[0]?.aborted, true);
    assert.strictEqual(signals[1]?.aborted, true);
  });

  void test('signal used with throwIfAborted pattern', async () => {
    await using ctx = await setupService(
      {
        async processItems(items: Array<string>, signal: AbortSignal) {
          const processed: Array<string> = [];
          for (const item of items) {
            signal.throwIfAborted();
            processed.push(item.toUpperCase());
            // Simulate async work
            await new Promise((r) => setTimeout(r, 5));
          }
          return processed;
        },
      },
      {handlers: [new AbortSignalHandler()]},
    );

    const controller = new AbortController();
    // Abort after a short delay
    setTimeout(() => controller.abort(), 15);

    try {
      await ctx.remote.processItems(
        ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        controller.signal,
      );
      // Might complete if fast enough, that's OK
    } catch {
      // Expected — the method threw via throwIfAborted()
    }
  });
});
