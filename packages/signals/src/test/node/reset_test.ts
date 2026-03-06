/**
 * Tests for Connection.reset() with SignalHandler.
 *
 * Verifies that SignalHandler state is properly cleaned up on
 * disconnect/reconnect, and that signals work correctly across
 * reset boundaries.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {Signal} from 'signal-polyfill';
import {Connection} from '@eamodio/supertalk-core';
import {SignalHandler, type RemoteSignal} from '../../index.js';
import {waitForMessages} from './test-utils.js';

void suite('SignalHandler with Connection.reset()', () => {
  void test('same signal object works after reset', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const count = new Signal.State(0);
    const service = {
      get count() {
        return count;
      },
      setCount(n: number) {
        count.set(n);
      },
    };

    const senderHandler = new SignalHandler({autoWatch: true});
    const receiverHandler1 = new SignalHandler({autoWatch: true});

    // Session 1
    const host = new Connection(ch1.port1, {handlers: [senderHandler]});
    host.expose(service);

    const wrap1 = new Connection(ch1.port2, {handlers: [receiverHandler1]});
    const remote1 = (await wrap1.waitForReady()) as typeof service;

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    const remoteCount1 = (await remote1.count) as unknown as RemoteSignal<number>;
    assert.strictEqual(remoteCount1.get(), 0);
    assert.strictEqual(senderHandler._sentSignalCount, 1);

    // Verify updates work in session 1
    count.set(5);
    await waitForMessages();
    assert.strictEqual(remoteCount1.get(), 5);

    // Tear down session 1 wrap side
    wrap1.close();
    ch1.port2.close();

    // Reset host with new endpoint
    host.reset(ch2.port1);

    // Sender handler should have been reset
    assert.strictEqual(senderHandler._sentSignalCount, 0);

    // Session 2 with new receiver
    const receiverHandler2 = new SignalHandler({autoWatch: true});
    host.expose(service);

    const wrap2 = new Connection(ch2.port2, {handlers: [receiverHandler2]});
    const remote2 = (await wrap2.waitForReady()) as typeof service;

    // Same signal object should work in new session
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    const remoteCount2 = (await remote2.count) as unknown as RemoteSignal<number>;
    assert.strictEqual(remoteCount2.get(), 5); // current value, not initial

    // Updates should flow in new session
    count.set(10);
    await waitForMessages();
    assert.strictEqual(remoteCount2.get(), 10);

    // Old RemoteSignal should NOT receive updates
    assert.strictEqual(remoteCount1.get(), 5); // still old value

    // Cleanup
    wrap2.close();
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('disconnect resets signal ID counter', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const sig1 = new Signal.State('a');
    const sig2 = new Signal.State('b');

    const senderHandler = new SignalHandler();

    // Session 1: send two signals to advance the counter
    const host = new Connection(ch1.port1, {handlers: [senderHandler]});
    host.expose({
      get sig1() {
        return sig1;
      },
      get sig2() {
        return sig2;
      },
    });

    const receiverHandler1 = new SignalHandler();
    const wrap1 = new Connection(ch1.port2, {handlers: [receiverHandler1]});
    const remote1 = (await wrap1.waitForReady()) as {
      sig1: Signal.State<string>;
      sig2: Signal.State<string>;
    };

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    await remote1.sig1;
    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    await remote1.sig2;
    assert.strictEqual(senderHandler._sentSignalCount, 2);

    // Tear down
    wrap1.close();
    ch1.port2.close();

    // Reset
    host.reset(ch2.port1);

    // After reset, counter should be back to 1
    assert.strictEqual(senderHandler._sentSignalCount, 0);

    // Session 2: send the same signals — they should get fresh IDs
    host.expose({
      get sig1() {
        return sig1;
      },
    });

    const receiverHandler2 = new SignalHandler();
    const wrap2 = new Connection(ch2.port2, {handlers: [receiverHandler2]});
    const remote2 = (await wrap2.waitForReady()) as {
      sig1: Signal.State<string>;
    };

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    const remoteSig = (await remote2.sig1) as unknown as RemoteSignal<string>;
    assert.strictEqual(remoteSig.get(), 'a');
    assert.strictEqual(senderHandler._sentSignalCount, 1);

    // Cleanup
    wrap2.close();
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });

  void test('watcher state is cleaned up on reset', async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();

    const count = new Signal.State(0);

    const senderHandler = new SignalHandler({autoWatch: true});

    // Session 1: send a signal with autoWatch (starts watching immediately)
    const host = new Connection(ch1.port1, {handlers: [senderHandler]});
    host.expose({
      get count() {
        return count;
      },
    });

    const receiverHandler1 = new SignalHandler({autoWatch: true});
    const wrap1 = new Connection(ch1.port2, {handlers: [receiverHandler1]});
    const remote1 = (await wrap1.waitForReady()) as {
      count: Signal.State<number>;
    };

    // eslint-disable-next-line @typescript-eslint/await-thenable -- proxy property is thenable at runtime
    await remote1.count;
    assert.strictEqual(senderHandler._isWatching(1), true);

    // Tear down and reset
    wrap1.close();
    ch1.port2.close();
    host.reset(ch2.port1);

    // Watcher state should be cleaned up — old signal ID 1 no longer watched
    assert.strictEqual(senderHandler._isWatching(1), false);
    assert.strictEqual(senderHandler._sentSignalCount, 0);

    // Cleanup
    host.close();
    ch1.port1.close();
    ch2.port1.close();
    ch2.port2.close();
  });
});
