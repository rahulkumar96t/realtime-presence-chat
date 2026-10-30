import { afterEach, describe, expect, it } from 'vitest';
import { metricValue, sleep, startNode, type TestNode, TestClient, token } from './helpers.js';

let node: TestNode | undefined;

afterEach(async () => {
  await node?.close();
  node = undefined;
});

describe('heartbeat', () => {
  it('terminates a connection that stops answering pings and announces its departure', async () => {
    // Intervals far below the 30 s default keep the test fast; 200 ms still tolerates the
    // event-loop stalls of a busy CI runner without failing healthy connections.
    node = await startNode({ heartbeatIntervalMs: 200 });
    const watcher = await TestClient.ready(node.wsUrl, await token('watcher'));
    await watcher.join('lobby');
    // autoPong: false makes this client ignore pings, like a peer whose network silently died.
    const silent = await TestClient.ready(node.wsUrl, await token('ghost'), { autoPong: false });
    await silent.join('lobby');
    await watcher.takeType('presence', (frame) => frame.event === 'join');

    expect((await silent.closed).code).toBe(1006);
    expect(await watcher.takeType('presence', (frame) => frame.event === 'leave')).toMatchObject({
      member: { userId: 'ghost' },
    });
    expect(await metricValue(node.httpUrl, /^chat_disconnects_total\{reason="heartbeat_timeout"\}/)).toBe(1);
    await watcher.close();
  });

  it('keeps responsive connections open across many intervals', async () => {
    node = await startNode({ heartbeatIntervalMs: 200 });
    const client = await TestClient.ready(node.wsUrl, await token('alive'));
    await sleep(1_100);
    expect(node.server.connectionCount).toBe(1);
    client.send({ type: 'ping' });
    expect(await client.takeType('pong')).toBeTruthy();
    await client.close();
  });
});

describe('backpressure', () => {
  it('disconnects a consumer that stops reading instead of buffering without bound', async () => {
    node = await startNode({
      rateLimit: { capacity: 100_000, refillPerSecond: 100_000, maxViolations: 100 },
      backpressure: { softLimitBytes: 16 * 1024, hardLimitBytes: 64 * 1024 },
    });
    const sender = await TestClient.ready(node.wsUrl, await token('sender'));
    const slow = await TestClient.ready(node.wsUrl, await token('slow'));
    await sender.join('firehose');
    await slow.join('firehose');
    await sender.takeType('presence');

    // Stop reading from the socket: the kernel buffers fill up, then the server's
    // bufferedAmount for this client grows past the hard limit.
    slow.socket.pause();
    // Linux auto-tunes loopback socket buffers to several MB, so allow plenty of traffic; the loop
    // stops as soon as the server has dropped the slow client.
    const text = 'x'.repeat(1_900);
    let sent = 0;
    while (sent < 20_000 && node.server.connectionCount === 2) {
      sender.send({ type: 'message', room: 'firehose', text });
      sent += 1;
      if (sent % 50 === 0) await sleep(5);
    }

    expect(node.server.connectionCount).toBe(1);
    expect(await metricValue(node.httpUrl, /^chat_disconnects_total\{reason="slow_consumer"\}/)).toBe(1);
    expect(await metricValue(node.httpUrl, /^chat_frames_dropped_total\{reason="slow_consumer"\}/)).toBeGreaterThan(0);

    // The fast member is unaffected and sees the slow one leave.
    expect(await sender.takeType('presence', (frame) => frame.event === 'leave', 5_000)).toMatchObject({
      member: { userId: 'slow' },
    });
    slow.socket.resume();
    expect((await slow.closed).code).toBe(1006);
    await sender.close();
  });
});
