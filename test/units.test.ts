import { WebSocket } from 'ws';
import { describe, expect, it, vi } from 'vitest';
import { ClientConnection } from '../src/connection.js';
import { ChatMetrics } from '../src/metrics.js';
import { parseClientMessage } from '../src/protocol.js';
import { TokenBucket } from '../src/rateLimiter.js';
import { RingBuffer } from '../src/ringBuffer.js';

describe('parseClientMessage', () => {
  it('accepts every v1 message type', () => {
    for (const frame of [
      { v: 1, type: 'join', room: 'lobby' },
      { v: 1, type: 'leave', room: 'lobby' },
      { v: 1, type: 'message', room: 'lobby', text: 'hi', clientMsgId: 'c-1' },
      { v: 1, type: 'typing', room: 'team_42-a', isTyping: true },
      { v: 1, type: 'ping' },
    ]) {
      expect(parseClientMessage(JSON.stringify(frame))).toMatchObject({ ok: true, message: frame });
    }
  });

  it('trims message text and rejects blank or oversized text', () => {
    expect(parseClientMessage('{"v":1,"type":"message","room":"a","text":"  hi  "}')).toMatchObject({
      ok: true,
      message: { text: 'hi' },
    });
    expect(parseClientMessage('{"v":1,"type":"message","room":"a","text":"   "}')).toMatchObject({
      ok: false,
      code: 'invalid_message',
    });
    const long = JSON.stringify({ v: 1, type: 'message', room: 'a', text: 'x'.repeat(2001) });
    expect(parseClientMessage(long)).toMatchObject({ ok: false, code: 'invalid_message' });
  });

  it('distinguishes bad JSON, wrong versions and invalid shapes', () => {
    expect(parseClientMessage('{nope')).toMatchObject({ ok: false, code: 'bad_json' });
    expect(parseClientMessage('[1,2]')).toMatchObject({ ok: false, code: 'invalid_message' });
    expect(parseClientMessage('"text"')).toMatchObject({ ok: false, code: 'invalid_message' });
    expect(parseClientMessage('{"type":"ping"}')).toMatchObject({ ok: false, code: 'unsupported_version' });
    expect(parseClientMessage('{"v":2,"type":"ping"}')).toMatchObject({ ok: false, code: 'unsupported_version' });
    expect(parseClientMessage('{"v":1,"type":"shout","room":"a"}')).toMatchObject({ ok: false, code: 'invalid_message' });
  });

  it.each(['', 'Lobby', '-lobby', 'a b', 'a'.repeat(65), '../etc'])('rejects room name %j', (room) => {
    expect(parseClientMessage(JSON.stringify({ v: 1, type: 'join', room }))).toMatchObject({ ok: false });
  });
});

describe('TokenBucket', () => {
  it('allows a burst up to capacity, then refills over time', () => {
    let now = 0;
    const bucket = new TokenBucket(3, 2, () => now);
    expect([1, 2, 3, 4].map(() => bucket.tryRemove())).toEqual([true, true, true, false]);
    expect(bucket.msUntilAvailable()).toBe(500);
    now += 499;
    expect(bucket.tryRemove()).toBe(false);
    now += 1;
    expect(bucket.tryRemove()).toBe(true);
  });

  it('never holds more than its capacity', () => {
    let now = 0;
    const bucket = new TokenBucket(5, 10, () => now);
    now += 60_000;
    expect(bucket.available).toBe(5);
  });

  it('tolerates a clock that goes backwards', () => {
    let now = 1_000;
    const bucket = new TokenBucket(1, 1, () => now);
    bucket.tryRemove();
    now = 0;
    expect(bucket.tryRemove()).toBe(false);
  });

  it('rejects non-positive settings', () => {
    expect(() => new TokenBucket(0, 1)).toThrow(RangeError);
    expect(() => new TokenBucket(1, 0)).toThrow(RangeError);
  });
});

describe('RingBuffer', () => {
  it('keeps the newest items in insertion order', () => {
    const ring = new RingBuffer<number>(3);
    expect(ring.toArray()).toEqual([]);
    [1, 2].forEach((n) => ring.push(n));
    expect(ring.toArray()).toEqual([1, 2]);
    [3, 4, 5].forEach((n) => ring.push(n));
    expect(ring.toArray()).toEqual([3, 4, 5]);
    expect(ring.length).toBe(3);
  });

  it('works with capacity one', () => {
    const ring = new RingBuffer<string>(1);
    ring.push('a');
    ring.push('b');
    expect(ring.toArray()).toEqual(['b']);
  });

  it('validates capacity', () => {
    expect(() => new RingBuffer(0)).toThrow(RangeError);
    expect(() => new RingBuffer(1.5)).toThrow(RangeError);
  });
});

describe('ClientConnection backpressure policy', () => {
  function fakeConnection(bufferedAmount: number) {
    const socket = { readyState: WebSocket.OPEN, bufferedAmount, send: vi.fn() };
    const onSlowConsumer = vi.fn();
    const connection = new ClientConnection('c1', { userId: 'u', name: 'U' }, socket as unknown as WebSocket, {
      backpressure: { softLimitBytes: 1_000, hardLimitBytes: 10_000 },
      rateLimit: { capacity: 1, refillPerSecond: 1 },
      metrics: new ChatMetrics({ collectProcessMetrics: false }),
      onSlowConsumer,
    });
    return { socket, connection, onSlowConsumer };
  }
  const frame = { v: 1, type: 'pong' } as const;

  it('sends everything while the buffer is below the soft limit', () => {
    const { socket, connection } = fakeConnection(999);
    expect(connection.send(frame, 'droppable')).toBe('sent');
    expect(socket.send).toHaveBeenCalledOnce();
  });

  it('drops droppable frames but keeps essential ones between the limits', () => {
    const { socket, connection, onSlowConsumer } = fakeConnection(5_000);
    expect(connection.send(frame, 'droppable')).toBe('dropped');
    expect(connection.send(frame, 'essential')).toBe('sent');
    expect(socket.send).toHaveBeenCalledOnce();
    expect(onSlowConsumer).not.toHaveBeenCalled();
  });

  it('reports a slow consumer once when the hard limit is exceeded', () => {
    const { socket, connection, onSlowConsumer } = fakeConnection(10_001);
    expect(connection.send(frame, 'essential')).toBe('disconnected');
    expect(connection.send(frame, 'essential')).toBe('disconnected');
    expect(onSlowConsumer).toHaveBeenCalledOnce();
    expect(socket.send).not.toHaveBeenCalled();
  });

  it('holds room events while a join snapshot is being assembled', () => {
    const { socket, connection } = fakeConnection(0);
    connection.syncing.set('lobby', []);
    connection.deliver('lobby', frame, 'essential');
    expect(socket.send).not.toHaveBeenCalled();
    expect(connection.syncing.get('lobby')).toHaveLength(1);
    connection.deliver('other', frame, 'essential');
    expect(socket.send).toHaveBeenCalledOnce();
  });

  it('runs queued work strictly in order, even after a failure', async () => {
    const { connection } = fakeConnection(0);
    const order: string[] = [];
    const slow = connection.enqueue(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      order.push('first');
      throw new Error('boom');
    });
    const fast = connection.enqueue(async () => {
      order.push('second');
    });
    await expect(slow).rejects.toThrow('boom');
    await fast;
    expect(order).toEqual(['first', 'second']);
  });
});
