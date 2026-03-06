import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {expose, wrap} from '../../index.js';

void suite('logger', () => {
  void test('logger.debug receives RPC call traces', async () => {
    const logs: Array<Array<unknown>> = [];
    const {port1, port2} = new MessageChannel();
    expose({greet: (name: string) => `Hello, ${name}!`}, port1, {
      logger: {debug: (...args: Array<unknown>) => logs.push(args)},
    });
    const remote = await wrap<{greet: (name: string) => string}>(port2);

    await remote.greet('world');

    assert.ok(logs.length > 0, 'should have debug logs');
    assert.ok(
      logs.some(
        (l) =>
          String(l[0]).includes('greet') && String(l[0]).includes('completed'),
      ),
      'should log method name and completion',
    );

    port1.close();
    port2.close();
  });

  void test('logger.debug logs call failures', async () => {
    const logs: Array<Array<unknown>> = [];
    const {port1, port2} = new MessageChannel();
    expose(
      {
        fail: () => {
          throw new Error('boom');
        },
      },
      port1,
      {logger: {debug: (...args: Array<unknown>) => logs.push(args)}},
    );
    const remote = await wrap<{fail: () => void}>(port2);

    try {
      await remote.fail();
    } catch {
      /* expected */
    }

    assert.ok(
      logs.some(
        (l) => String(l[0]).includes('fail') && String(l[0]).includes('failed'),
      ),
      'should log method name and failure',
    );

    port1.close();
    port2.close();
  });

  void test('no logging overhead when logger is not provided', async () => {
    // Just verify it works without a logger (no crashes)
    const {port1, port2} = new MessageChannel();
    expose({greet: () => 'hi'}, port1);
    const remote = await wrap<{greet: () => string}>(port2);

    const result = await remote.greet();
    assert.strictEqual(result, 'hi');

    port1.close();
    port2.close();
  });

  void test('logger.debug includes duration info', async () => {
    const logs: Array<Array<unknown>> = [];
    const {port1, port2} = new MessageChannel();
    expose(
      {
        slow: async () => {
          await new Promise((r) => setTimeout(r, 10));
          return 'done';
        },
      },
      port1,
      {logger: {debug: (...args: Array<unknown>) => logs.push(args)}},
    );
    const remote = await wrap<{slow: () => Promise<string>}>(port2);

    await remote.slow();

    const callLog = logs.find(
      (l) =>
        String(l[0]).includes('slow') && String(l[0]).includes('completed'),
    );
    assert.ok(callLog, 'should have a completion log for slow()');
    // Second arg should be an object with duration
    const meta = callLog[1] as {duration?: number} | undefined;
    assert.ok(meta?.duration != null, 'should include duration');
    assert.ok(
      meta.duration >= 5,
      'duration should reflect actual execution time',
    );

    port1.close();
    port2.close();
  });
});
