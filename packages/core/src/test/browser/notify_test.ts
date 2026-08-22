import {expose, wrap, notify} from '../../index.js';
import type {Endpoint} from '../../index.js';

/**
 * Wraps a MessagePort and records postMessage calls.
 */
function spyEndpoint(port: Endpoint): Endpoint & {messages: Array<unknown>} {
  const messages: Array<unknown> = [];
  return {
    messages,
    postMessage(message: unknown, transfer?: Array<Transferable>) {
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

// Using @web/test-runner with mocha's tdd interface (suite/test)
suite('notify (browser)', () => {
  test('one message, no response, over a MessageChannel', async () => {
    const {port1, port2} = new MessageChannel();

    // In browser, MessagePort needs start() to be called before messages flow.
    port1.start();
    port2.start();

    // Spy on port1 (the receiver) so we can assert it never posts a
    // response back for the notify.
    const spy1 = spyEndpoint(port1);

    const calls: Array<string> = [];
    const service = {
      greet(name: string): void {
        calls.push(name);
      },
      ping(): string {
        return 'pong';
      },
    };

    expose(service, spy1);
    const remote = await wrap<typeof service>(port2);

    spy1.messages.length = 0;

    notify(remote).greet('world');

    // Round-trip a normal call so the notify is guaranteed to have been
    // processed (messages are delivered and handled in order).
    await remote.ping();

    if (calls.length !== 1 || calls[0] !== 'world') {
      throw new Error(
        `Expected the remote method to run, got ${String(calls)}`,
      );
    }

    if (spy1.messages.some((m) => (m as {id?: number}).id === -1)) {
      throw new Error('The receiver must never post a response for a notify');
    }

    port1.close();
    port2.close();
  });

  test('notify() on a callback function proxy invokes the local callback', async () => {
    const {port1, port2} = new MessageChannel();

    port1.start();
    port2.start();

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
    await new Promise((resolve) => setTimeout(resolve, 10));

    if (received.length !== 1 || received[0] !== 42) {
      throw new Error(
        `Expected the callback to run with 42, got ${String(received)}`,
      );
    }

    port1.close();
    port2.close();
  });
});
