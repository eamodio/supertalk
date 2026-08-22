import {expose, wrap} from '../../index.js';
import type {Endpoint} from '../../index.js';
import {SequencedChannel, type ChannelGap} from '../../handlers/channel.js';

interface HandlerEnvelope {
  type: string;
  wireType?: string;
  payload?: unknown;
}

/**
 * Wraps a MessagePort and can drop the next matching handler message, so a
 * real gap can be created deterministically.
 */
function lossyEndpoint(port: Endpoint): Endpoint & {
  dropNextHandlerMessage(
    wireType: string,
    match: (payload: unknown) => boolean,
  ): void;
} {
  const drops: Array<{
    wireType: string;
    match: (payload: unknown) => boolean;
  }> = [];
  return {
    dropNextHandlerMessage(wireType, match) {
      drops.push({wireType, match});
    },
    postMessage(message: unknown, transfer?: Array<Transferable>) {
      const env = message as HandlerEnvelope;
      if (env.type === 'handler' && env.wireType !== undefined) {
        const idx = drops.findIndex(
          (d) => d.wireType === env.wireType && d.match(env.payload),
        );
        if (idx !== -1) {
          drops.splice(idx, 1);
          return; // dropped
        }
      }
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

function isDataEnvelope(payload: unknown): boolean {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    's' in payload &&
    !('r' in payload) &&
    !('m' in payload) &&
    !('e' in payload)
  );
}

function isEpochEnvelope(payload: unknown): boolean {
  return typeof payload === 'object' && payload !== null && 'e' in payload;
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor: condition not met within timeout');
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

// Using @web/test-runner with mocha's tdd interface (suite/test)
suite('SequencedChannel (browser)', () => {
  test('ordered delivery over a MessageChannel', async () => {
    const {port1, port2} = new MessageChannel();
    port1.start();
    port2.start();

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, port1, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));

    senderCh.send('a');
    senderCh.send('b');
    senderCh.send('c');

    await waitFor(() => received.length === 3);

    if (received.join(',') !== 'a,b,c') {
      throw new Error(`Expected ordered delivery, got ${received.join(',')}`);
    }

    port1.close();
    port2.close();
  });

  test('gap with no replay configured surfaces onGap and stops delivery', async () => {
    const {port1, port2} = new MessageChannel();
    port1.start();
    port2.start();

    // Wrap the SENDER's endpoint — data messages travel sender -> receiver,
    // so dropping one requires intercepting the sender's outbound postMessage.
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    });

    senderCh.send('a'); // seq 0 — delivered
    senderCh.send('b'); // seq 1 — dropped
    senderCh.send('c'); // seq 2 — triggers gap detection

    await waitFor(() => gaps.length === 1);

    if (received.join(',') !== 'a') {
      throw new Error(
        `Expected only the pre-gap message to be delivered, got ${received.join(',')}`,
      );
    }
    if (!receiverCh.gapped) {
      throw new Error('Expected the receiver to be gapped');
    }
    const gap = gaps[0];
    if (gap?.generation !== 0 || gap.expected !== 1 || gap.received !== 2) {
      throw new Error(`Unexpected gap payload: ${JSON.stringify(gap)}`);
    }

    port1.close();
    port2.close();
  });

  test('cross-generation gap (mid-stream join) with no replay coverage reports onGap', async () => {
    const {port1, port2} = new MessageChannel();
    port1.start();
    port2.start();

    const lossy = lossyEndpoint(port1);

    // No `replay` option: the sender can never repair a mid-stream join.
    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    senderCh.send('a'); // gen 0, seq 0
    await waitFor(() => received.length === 1);

    // Drop the epoch announcement AND generation 1's seq 0, so the first
    // thing the receiver sees of generation 1 is seq 1 — a mid-stream join.
    lossy.dropNextHandlerMessage(senderCh.wireType, isEpochEnvelope);
    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 0;
    });

    senderCh.newGeneration();
    senderCh.send('x'); // gen 1, seq 0 — dropped
    senderCh.send('y'); // gen 1, seq 1 — mid-stream join, no replay coverage

    await waitFor(() => gaps.length === 1);

    if (received.join(',') !== 'a') {
      throw new Error(
        `Expected only the pre-generation message to be delivered, got ${received.join(',')}`,
      );
    }
    if (!receiverCh.gapped) {
      throw new Error('Expected the receiver to be gapped');
    }
    const gap = gaps[0];
    if (gap?.generation !== 1 || gap.expected !== 0 || gap.received !== 1) {
      throw new Error(`Unexpected gap payload: ${JSON.stringify(gap)}`);
    }

    port1.close();
    port2.close();
  });
});
