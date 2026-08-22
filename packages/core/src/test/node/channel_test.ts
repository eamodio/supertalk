/**
 * Tests for SequencedChannel — ordered delivery, gap detection, replay, and
 * generations.
 */

import {suite, test} from 'node:test';
import * as assert from 'node:assert';
import {MessageChannel} from 'node:worker_threads';
import {expose, wrap, Connection} from '../../index.js';
import type {Endpoint} from '../../index.js';
import {
  SequencedChannel,
  type ChannelMeta,
  type ChannelGap,
} from '../../handlers/channel.js';

interface HandlerEnvelope {
  type: string;
  wireType?: string;
  payload?: unknown;
}

/**
 * Wraps an Endpoint and can drop the next N messages matching a predicate
 * over the handler envelope, so a real gap can be created deterministically.
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

/** Wraps an Endpoint and records every posted message. */
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

/** Poll until `condition()` is true or the timeout elapses. */
async function waitFor(
  condition: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor: condition not met within timeout');
    }
    await new Promise((r) => setTimeout(r, 5));
  }
}

void suite('SequencedChannel', () => {
  void test('ordered delivery: seq increments, generation stable', async () => {
    const {port1, port2} = new MessageChannel();

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, port1, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<[string, ChannelMeta]> = [];
    receiverCh.subscribe((v, meta) => received.push([v, meta]));

    senderCh.send('a');
    senderCh.send('b');
    senderCh.send('c');

    await waitFor(() => received.length === 3);

    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'b', 'c'],
    );
    assert.deepStrictEqual(
      received.map(([, meta]) => meta.seq),
      [0, 1, 2],
    );
    assert.ok(received.every(([, meta]) => meta.generation === 0));

    port1.close();
    port2.close();
  });

  void test('gap with no replay configured surfaces onGap after the miss round trip, nothing further delivered', async () => {
    const {port1, port2} = new MessageChannel();
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

    // Drop seq 1 (the second message) so the receiver sees 0, then 2.
    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    });

    senderCh.send('a'); // seq 0 — delivered
    senderCh.send('b'); // seq 1 — dropped
    senderCh.send('c'); // seq 2 — triggers gap detection

    await waitFor(() => gaps.length === 1);

    assert.deepStrictEqual(received, ['a']);
    assert.strictEqual(receiverCh.gapped, true);
    assert.deepStrictEqual(gaps[0], {generation: 0, expected: 1, received: 2});

    // Nothing further is delivered while gapped.
    senderCh.send('d');
    await new Promise((r) => setTimeout(r, 30));
    assert.deepStrictEqual(received, ['a']);

    port1.close();
    port2.close();
  });

  void test('onGap fires exactly once per gap, and re-arms for a new gap in the next generation', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    // Drop seq 1 so the receiver gaps at seq 2.
    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    });

    senderCh.send('a'); // seq 0 — delivered
    senderCh.send('b'); // seq 1 — dropped
    senderCh.send('c'); // seq 2 — opens the gap
    await waitFor(() => gaps.length === 1);

    // Every further live message re-arms recovery (a new replay request and
    // miss round trip), but the SAME gap must not report again.
    senderCh.send('d');
    senderCh.send('e');
    await new Promise((r) => setTimeout(r, 30));
    assert.strictEqual(gaps.length, 1, 'one gap reports exactly once');

    // A new generation clears the gap; a fresh gap in it reports again.
    senderCh.newGeneration();
    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    });
    senderCh.send('f'); // gen 1, seq 0 — delivered, clears the old gap
    senderCh.send('g'); // gen 1, seq 1 — dropped
    senderCh.send('h'); // gen 1, seq 2 — opens a new gap
    await waitFor(() => gaps.length === 2);
    assert.deepStrictEqual(gaps[1], {generation: 1, expected: 1, received: 2});

    port1.close();
    port2.close();
  });

  void test('replay repairs a gap end-to-end: no gap event, every message delivered in order', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows', {replay: 32});
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

    senderCh.send('a');
    senderCh.send('b'); // dropped, then replayed
    senderCh.send('c');
    senderCh.send('d');

    await waitFor(() => received.length === 4);

    assert.deepStrictEqual(received, ['a', 'b', 'c', 'd']);
    assert.strictEqual(gaps.length, 0);
    assert.strictEqual(receiverCh.gapped, false);

    port1.close();
    port2.close();
  });

  void test('replay miss (drop older than the buffer) emits onGap', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    // Buffer only holds 1 entry, so by the time the receiver detects the
    // gap and requests a replay of seq 1, the sender's buffer has already
    // moved past it: send('b') evicts seq 0 to make room, then send('c')
    // evicts seq 1 itself, so the requested seq has fallen out of the
    // window by the time the request arrives.
    const senderCh = new SequencedChannel<string>('rows', {replay: 1});
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

    senderCh.send('a'); // seq 0 — delivered, establishes the baseline
    senderCh.send('b'); // seq 1 — dropped, and pushed out of the size-1 buffer
    senderCh.send('c'); // seq 2 — gap detected, replay requested, buffer no longer covers seq 1

    await waitFor(() => gaps.length === 1);

    assert.deepStrictEqual(received, ['a']);
    assert.strictEqual(receiverCh.gapped, true);
    assert.deepStrictEqual(gaps[0], {generation: 0, expected: 1, received: 2});

    port1.close();
    port2.close();
  });

  void test('cross-generation gap (mid-stream join) heals via replay, no gap event', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows', {replay: 32});
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<[string, ChannelMeta]> = [];
    receiverCh.subscribe((v, meta) => received.push([v, meta]));
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
    senderCh.send('y'); // gen 1, seq 1 — mid-stream join, triggers a replay from seq 0

    await waitFor(() => received.length === 3);

    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'x', 'y'],
    );
    assert.strictEqual(gaps.length, 0);
    assert.strictEqual(receiverCh.gapped, false);
    const genOneEntries = received.slice(1);
    assert.deepStrictEqual(
      genOneEntries.map(([, meta]) => [meta.generation, meta.seq]),
      [
        [1, 0],
        [1, 1],
      ],
    );

    port1.close();
    port2.close();
  });

  void test('cross-generation gap (mid-stream join) with no replay coverage reports onGap', async () => {
    const {port1, port2} = new MessageChannel();
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

    lossy.dropNextHandlerMessage(senderCh.wireType, isEpochEnvelope);
    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 0;
    });

    senderCh.newGeneration();
    senderCh.send('x'); // gen 1, seq 0 — dropped
    senderCh.send('y'); // gen 1, seq 1 — mid-stream join, no replay coverage

    await waitFor(() => gaps.length === 1);

    assert.deepStrictEqual(received, ['a']);
    assert.strictEqual(receiverCh.gapped, true);
    assert.deepStrictEqual(gaps[0], {generation: 1, expected: 0, received: 1});

    // Nothing from the new generation is delivered while gapped.
    senderCh.send('z');
    await new Promise((r) => setTimeout(r, 30));
    assert.deepStrictEqual(received, ['a']);

    port1.close();
    port2.close();
  });

  void test('a lost replay re-arms recovery on the next live message', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows', {replay: 32});
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    const isSeq1 = (p: unknown) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    };
    // Drop seq 1's live send, then also drop the sender's replayed copy of
    // seq 1 (same {g, s} as the original, so register the drop separately)
    // so the first repair attempt fails silently.
    lossy.dropNextHandlerMessage(senderCh.wireType, isSeq1);
    lossy.dropNextHandlerMessage(senderCh.wireType, isSeq1);

    senderCh.send('a'); // seq 0 — delivered
    senderCh.send('b'); // seq 1 — dropped, opens the gap
    senderCh.send('c'); // seq 2 — gap detected, replay of seq 1 requested and then also dropped

    // Give the (failed) replay round trip time to complete: still gapped,
    // nothing new delivered, and no onGap — the miss was never reported
    // because the replay response never arrived at all.
    await new Promise((r) => setTimeout(r, 60));
    assert.deepStrictEqual(received, ['a']);
    assert.strictEqual(receiverCh.gapped, true);
    assert.strictEqual(gaps.length, 0);

    senderCh.send('d'); // seq 3 — a new live message past the last request; re-arms recovery

    await waitFor(() => received.length === 4);

    assert.deepStrictEqual(received, ['a', 'b', 'c', 'd']);
    assert.strictEqual(gaps.length, 0);
    assert.strictEqual(receiverCh.gapped, false);

    port1.close();
    port2.close();
  });

  void test('newGeneration() resets seq, clears a gapped receiver, and delivers again', async () => {
    const {port1, port2} = new MessageChannel();
    const lossy = lossyEndpoint(port1);

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, lossy, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<[string, ChannelMeta]> = [];
    receiverCh.subscribe((v, meta) => received.push([v, meta]));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    lossy.dropNextHandlerMessage(senderCh.wireType, (p) => {
      const payload = p as {s?: number};
      return isDataEnvelope(p) && payload.s === 1;
    });

    senderCh.send('a');
    senderCh.send('b'); // dropped
    senderCh.send('c'); // gap

    await waitFor(() => gaps.length === 1);
    assert.strictEqual(receiverCh.gapped, true);

    const newGen = senderCh.newGeneration();
    assert.strictEqual(newGen, 1);

    senderCh.send('x'); // seq 0 of generation 1

    await waitFor(() => received.length === 2);

    assert.strictEqual(receiverCh.gapped, false);
    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'x'],
    );
    const secondEntry = received[1];
    assert.ok(secondEntry !== undefined);
    const [, meta] = secondEntry;
    assert.strictEqual(meta.generation, 1);
    assert.strictEqual(meta.seq, 0);

    port1.close();
    port2.close();
  });

  void test('messages from an older generation arriving after a bump are dropped', async () => {
    const {port1, port2} = new MessageChannel();

    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    expose({}, port1, {handlers: [senderCh]});
    await wrap(port2, {handlers: [receiverCh]});

    const received: Array<[string, ChannelMeta]> = [];
    receiverCh.subscribe((v, meta) => received.push([v, meta]));

    senderCh.send('a');
    await waitFor(() => received.length === 1);

    senderCh.newGeneration();
    senderCh.send('b'); // gen 1, seq 0

    await waitFor(() => received.length === 2);
    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'b'],
    );

    // Manually deliver a stale generation-0 message directly to the
    // receiver handler to simulate a slow in-flight message arriving late.
    receiverCh.onMessage({g: 0, s: 5, v: 'stale'});
    await new Promise((r) => setTimeout(r, 20));

    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'b'],
    );

    port1.close();
    port2.close();
  });

  void test('duplicate seq is dropped', () => {
    const receiverCh = new SequencedChannel<string>('rows');
    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));

    receiverCh.onMessage({g: 0, s: 0, v: 'a'});
    receiverCh.onMessage({g: 0, s: 1, v: 'b'});
    receiverCh.onMessage({g: 0, s: 1, v: 'b-dup'});
    receiverCh.onMessage({g: 0, s: 0, v: 'a-dup'});

    assert.deepStrictEqual(received, ['a', 'b']);
  });

  void test('a duplicate/stale epoch announcement is ignored: adopted generation is not re-reset', () => {
    const receiverCh = new SequencedChannel<string>('rows');
    const received: Array<string> = [];
    receiverCh.subscribe((v) => received.push(v));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    receiverCh.onMessage({g: 0, e: 1}); // adopt generation 0
    receiverCh.onMessage({g: 0, s: 0, v: 'a'});
    receiverCh.onMessage({g: 0, s: 1, v: 'b'});

    // A duplicate epoch announcement for the already-adopted generation
    // arrives again (reordered or redelivered) — must not reset #expected
    // back to 0.
    receiverCh.onMessage({g: 0, e: 1});
    // A stale (lower) generation's epoch announcement — also ignored.
    receiverCh.onMessage({g: -1, e: 1});

    // The next in-sequence message still delivers normally. If #expected
    // had been reset to 0, this would be treated as a gap instead.
    receiverCh.onMessage({g: 0, s: 2, v: 'c'});

    assert.deepStrictEqual(received, ['a', 'b', 'c']);
    assert.strictEqual(gaps.length, 0, 'no spurious gap from the re-adoption');
  });

  void test('a stale replay miss (wrong generation or wrong expected seq) does not fire onGap', () => {
    const receiverCh = new SequencedChannel<string>('rows');
    receiverCh.subscribe(() => {
      // no-op
    });
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    // Adopt generation 0, deliver seq 0, then open a gap at seq 2 (expected
    // seq 1 is missing).
    receiverCh.onMessage({g: 0, s: 0, v: 'a'});
    receiverCh.onMessage({g: 0, s: 2, v: 'c'});
    assert.strictEqual(receiverCh.gapped, true);

    // A miss for a different generation than the one currently gapped.
    receiverCh.onMessage({g: 1, m: 1});
    // A miss for the right generation but the wrong (stale) expected seq.
    receiverCh.onMessage({g: 0, m: 5});

    assert.strictEqual(gaps.length, 0, 'neither stale miss should fire onGap');

    // The correctly correlated miss still fires it.
    receiverCh.onMessage({g: 0, m: 1});
    assert.strictEqual(gaps.length, 1);
  });

  void test('bidirectional use on a single pair of instances', async () => {
    const {port1, port2} = new MessageChannel();

    const chA = new SequencedChannel<string>('duplex');
    const chB = new SequencedChannel<string>('duplex');

    expose({}, port1, {handlers: [chA]});
    await wrap(port2, {handlers: [chB]});

    const receivedByB: Array<string> = [];
    chB.subscribe((v) => receivedByB.push(v));
    const receivedByA: Array<string> = [];
    chA.subscribe((v) => receivedByA.push(v));

    chA.send('from-a-1');
    chA.send('from-a-2');
    chB.send('from-b-1');

    await waitFor(() => receivedByB.length === 2 && receivedByA.length === 1);

    assert.deepStrictEqual(receivedByB, ['from-a-1', 'from-a-2']);
    assert.deepStrictEqual(receivedByA, ['from-b-1']);

    port1.close();
    port2.close();
  });

  void test('batching: N send()s in one microtask = one postMessage, order preserved', async () => {
    const {port1, port2} = new MessageChannel();
    const spy = spyEndpoint(port2);

    const senderCh = new SequencedChannel<number>('nums');
    const receiverCh = new SequencedChannel<number>('nums');

    expose({}, spy, {handlers: [senderCh], batching: true});
    await wrap(port1, {handlers: [receiverCh]});

    const received: Array<number> = [];
    receiverCh.subscribe((v) => received.push(v));

    spy.messages.length = 0; // clear handshake traffic

    senderCh.send(1);
    senderCh.send(2);
    senderCh.send(3);

    await waitFor(() => received.length === 3);

    assert.deepStrictEqual(received, [1, 2, 3]);
    assert.strictEqual(
      spy.messages.length,
      1,
      'three synchronous sends should coalesce into a single postMessage',
    );
    const batch = spy.messages[0] as {type: string; messages: Array<unknown>};
    assert.strictEqual(batch.type, 'batch');
    assert.strictEqual(batch.messages.length, 3);

    port1.close();
    port2.close();
  });

  void test('reconnect: Connection.reset() starts a new generation, receiver resyncs with no gap event', async () => {
    const {port1, port2} = new MessageChannel();

    // Same channel instances reused across the reset — that is the
    // scenario disconnect() exists for.
    const senderCh = new SequencedChannel<string>('rows');
    const receiverCh = new SequencedChannel<string>('rows');

    const host = new Connection(port1, {handlers: [senderCh]});
    host.expose({});
    const wrapConn = new Connection(port2, {handlers: [receiverCh]});
    await wrapConn.waitForReady();

    const received: Array<[string, ChannelMeta]> = [];
    receiverCh.subscribe((v, meta) => received.push([v, meta]));
    const gaps: Array<ChannelGap> = [];
    receiverCh.onGap((g) => gaps.push(g));

    senderCh.send('a'); // gen 0, seq 0
    await waitFor(() => received.length === 1);
    assert.strictEqual(senderCh.generation, 0);

    // Reset both sides on the same ports (mirrors reset_test.ts's "reset
    // without endpoint preserves listener").
    host.reset();
    host.expose({});
    wrapConn.reset();
    await wrapConn.waitForReady();

    assert.strictEqual(
      senderCh.generation,
      1,
      'disconnect() during reset should bump the outbound generation',
    );

    senderCh.send('b'); // gen 1, seq 0
    await waitFor(() => received.length === 2);

    assert.deepStrictEqual(
      received.map(([v]) => v),
      ['a', 'b'],
    );
    assert.strictEqual(gaps.length, 0, 'no spurious gap across a reconnect');
    assert.strictEqual(receiverCh.gapped, false);
    const secondEntry = received[1];
    assert.ok(secondEntry !== undefined);
    const [, meta] = secondEntry;
    assert.strictEqual(meta.generation, 1);
    assert.strictEqual(meta.seq, 0);

    wrapConn.close();
    host.close();
    port1.close();
    port2.close();
  });

  void test('multiple subscribe() listeners; unsubscribe stops one without affecting the other', () => {
    const receiverCh = new SequencedChannel<string>('rows');

    const receivedA: Array<string> = [];
    const receivedB: Array<string> = [];
    const unsubA = receiverCh.subscribe((v) => receivedA.push(v));
    receiverCh.subscribe((v) => receivedB.push(v));

    receiverCh.onMessage({g: 0, s: 0, v: 'a'});
    unsubA();
    receiverCh.onMessage({g: 0, s: 1, v: 'b'});

    assert.deepStrictEqual(receivedA, ['a']);
    assert.deepStrictEqual(receivedB, ['a', 'b']);
  });

  void test('a throwing listener does not corrupt state or block other listeners', () => {
    const receiverCh = new SequencedChannel<string>('rows');

    const receivedGood: Array<string> = [];
    receiverCh.subscribe(() => {
      throw new Error('boom');
    });
    receiverCh.subscribe((v) => receivedGood.push(v));

    receiverCh.onMessage({g: 0, s: 0, v: 'a'});
    receiverCh.onMessage({g: 0, s: 1, v: 'b'});

    assert.deepStrictEqual(receivedGood, ['a', 'b']);
  });

  void test('send() while not connected does not consume a sequence number', () => {
    const senderCh = new SequencedChannel<string>('rows');
    const sent: Array<unknown> = [];

    // Not connected yet — send() is a no-op.
    senderCh.send('lost');
    assert.strictEqual(sent.length, 0);

    senderCh.connect({sendMessage: (p) => sent.push(p)});
    senderCh.send('first');

    assert.strictEqual(sent.length, 1);
    assert.deepStrictEqual(sent[0], {g: 0, s: 0, v: 'first'});
  });

  void test('canHandle() always returns false, toWire() throws if reached', () => {
    const ch = new SequencedChannel<string>('rows');
    assert.strictEqual(ch.canHandle('anything'), false);
    assert.strictEqual(ch.canHandle({}), false);
    assert.throws(() => ch.toWire(), /SequencedChannel/);
  });
});
