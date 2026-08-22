import {expose, wrap, subscribe} from '../../index.js';

function tick(ms = 15): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Using @web/test-runner with mocha's tdd interface (suite/test)
suite('subscribe() (browser)', () => {
  test('subscribe-then-receive over a MessageChannel', async () => {
    const {port1, port2} = new MessageChannel();

    // In browser, MessagePort needs start() to be called before messages flow.
    port1.start();
    port2.start();

    const cbs = new Set<(n: number) => void>();
    const service = {
      onTick(cb: (n: number) => void): () => void {
        cbs.add(cb);
        return () => {
          cbs.delete(cb);
        };
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2);

    const received: Array<number> = [];
    const subscription = subscribe(remote, (r) =>
      r.onTick((n: number) => received.push(n)),
    );
    await subscription.ready;

    for (const cb of cbs) cb(1);
    await tick();

    if (received.length !== 1 || received[0] !== 1) {
      throw new Error(
        `Expected the subscriber to receive [1], got ${String(received)}`,
      );
    }

    subscription.unsubscribe();
    port1.close();
    port2.close();
  });

  test('unsubscribe() stops delivery and releases the remote side', async () => {
    const {port1, port2} = new MessageChannel();

    port1.start();
    port2.start();

    const cbs = new Set<(n: number) => void>();
    // Wrapped in a function so each read is opaque to control-flow
    // narrowing — `cbs` is mutated from a message handler TS can't see.
    const listenerCount = (): number => cbs.size;
    const service = {
      onTick(cb: (n: number) => void): () => void {
        cbs.add(cb);
        return () => {
          cbs.delete(cb);
        };
      },
    };

    expose(service, port1);
    const remote = await wrap<typeof service>(port2);

    const received: Array<number> = [];
    const subscription = subscribe(remote, (r) =>
      r.onTick((n: number) => received.push(n)),
    );
    await subscription.ready;

    if (listenerCount() !== 1) {
      throw new Error(
        `Expected one remote listener, got ${String(listenerCount())}`,
      );
    }

    subscription.unsubscribe();
    await tick();

    if (listenerCount() !== 0) {
      throw new Error(
        `Expected unsubscribe() to release the remote listener, got ${String(listenerCount())} remaining`,
      );
    }

    for (const cb of cbs) cb(2);
    await tick();

    if (received.length !== 0) {
      throw new Error(
        `Expected no delivery after unsubscribe, got ${String(received)}`,
      );
    }

    port1.close();
    port2.close();
  });
});
