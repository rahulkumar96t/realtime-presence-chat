import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import RedisMock from 'ioredis-mock';
import { afterEach, describe, expect, it } from 'vitest';
import type { ClusterAdapter, RoomEvent } from '../src/cluster/adapter.js';
import { InMemoryAdapter, InMemoryBroker } from '../src/cluster/memory.js';
import { RedisAdapter } from '../src/cluster/redis.js';
import type { ChatMessage } from '../src/protocol.js';
import { sleep } from './helpers.js';

/**
 * One contract, three backends. Each factory returns two adapters that behave like two server
 * nodes connected to the same backend.
 */
interface Backend {
  name: string;
  pair(options?: { historySize?: number }): Promise<[ClusterAdapter, ClusterAdapter]>;
}

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

/** True when a real Redis answers PING quickly; the real-Redis suite is skipped otherwise. */
async function redisReachable(url: string): Promise<boolean> {
  const probe = new Redis(url, {
    lazyConnect: true,
    connectTimeout: 500,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
    enableOfflineQueue: false,
  });
  probe.on('error', () => undefined);
  try {
    await probe.connect();
    return (await probe.ping()) === 'PONG';
  } catch {
    return false;
  } finally {
    probe.disconnect();
  }
}

const realRedis = await redisReachable(REDIS_URL);
const opened: ClusterAdapter[] = [];

afterEach(async () => {
  await Promise.all(opened.splice(0).map((adapter) => adapter.close()));
});

async function redisPair(
  connect: () => Redis,
  historySize: number,
): Promise<[ClusterAdapter, ClusterAdapter]> {
  // A fresh key prefix isolates each test on a shared server.
  const keyPrefix = `test:${randomUUID()}:`;
  const make = async () => {
    const publisher = connect();
    const adapter = new RedisAdapter({ publisher, subscriber: publisher.duplicate(), keyPrefix, historySize });
    await adapter.start();
    opened.push(adapter);
    return adapter;
  };
  return [await make(), await make()];
}

const backends: Array<Backend & { enabled: boolean }> = [
  {
    name: 'in-memory broker',
    enabled: true,
    pair: async ({ historySize = 50 } = {}) => {
      const broker = new InMemoryBroker(historySize);
      const pair: [ClusterAdapter, ClusterAdapter] = [new InMemoryAdapter(broker), new InMemoryAdapter(broker)];
      opened.push(...pair);
      return pair;
    },
  },
  {
    name: 'Redis adapter on ioredis-mock',
    enabled: true,
    pair: ({ historySize = 50 } = {}) => {
      const host = `mock-${randomUUID()}`;
      return redisPair(() => new RedisMock({ host }) as unknown as Redis, historySize);
    },
  },
  {
    name: `Redis adapter on a real Redis (${REDIS_URL})`,
    enabled: realRedis,
    pair: ({ historySize = 50 } = {}) => redisPair(() => new Redis(REDIS_URL, { maxRetriesPerRequest: 1 }), historySize),
  },
];

const message = (room: string, text: string): ChatMessage => ({
  id: randomUUID(),
  room,
  user: { userId: 'u1', name: 'User One' },
  text,
  sentAt: new Date().toISOString(),
});

async function until(condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await sleep(5);
  }
}

for (const backend of backends) {
  describe.skipIf(!backend.enabled)(`cluster adapter contract: ${backend.name}`, () => {
    it('delivers published events to every subscribed node, including the publisher', async () => {
      const [a, b] = await backend.pair();
      const seenA: RoomEvent[] = [];
      const seenB: RoomEvent[] = [];
      await a.subscribe('lobby', (event) => seenA.push(event));
      await b.subscribe('lobby', (event) => seenB.push(event));

      const event: RoomEvent = { kind: 'message', message: message('lobby', 'hello') };
      await b.publish('lobby', event);
      await until(() => seenA.length === 1 && seenB.length === 1);
      expect(seenA[0]).toEqual(event);
      expect(seenA[0]).not.toBe(event);
    });

    it('does not deliver other rooms or after unsubscribe', async () => {
      const [a, b] = await backend.pair();
      const seen: RoomEvent[] = [];
      await a.subscribe('lobby', (event) => seen.push(event));
      await b.publish('elsewhere', { kind: 'message', message: message('elsewhere', 'x') });
      await a.unsubscribe('lobby');
      await b.publish('lobby', { kind: 'message', message: message('lobby', 'y') });
      await sleep(100);
      expect(seen).toEqual([]);
    });

    it('keeps the most recent messages per room, oldest first, shared by all nodes', async () => {
      const [a, b] = await backend.pair({ historySize: 3 });
      for (const text of ['1', '2', '3', '4']) await a.appendHistory(message('lobby', text));
      await b.appendHistory(message('other', 'x'));
      expect((await b.history('lobby')).map((item) => item.text)).toEqual(['2', '3', '4']);
      expect((await a.history('other')).map((item) => item.text)).toEqual(['x']);
      expect(await a.history('empty')).toEqual([]);
    });

    it('reports presence changes per user, not per connection, across nodes', async () => {
      const [a, b] = await backend.pair();
      const dana = { userId: 'dana', name: 'Dana' };
      expect(await a.presenceJoin('lobby', 'c1', dana)).toBe(true);
      expect(await b.presenceJoin('lobby', 'c2', dana)).toBe(false);
      expect(await b.presenceJoin('lobby', 'c3', { userId: 'ada', name: 'Ada' })).toBe(true);
      expect(await a.presenceList('lobby')).toEqual([
        { userId: 'ada', name: 'Ada' },
        { userId: 'dana', name: 'Dana' },
      ]);

      expect(await a.presenceLeave('lobby', 'c1', 'dana')).toBe(false);
      expect(await b.presenceLeave('lobby', 'c2', 'dana')).toBe(true);
      expect(await a.presenceList('lobby')).toEqual([{ userId: 'ada', name: 'Ada' }]);
      // Leaving twice, or leaving an unknown connection, changes nothing.
      expect(await b.presenceLeave('lobby', 'c2', 'dana')).toBe(false);
      expect(await a.presenceLeave('nowhere', 'c9', 'nobody')).toBe(false);
    });

    it('produces exactly one join event when two nodes add the same user concurrently', async () => {
      const [a, b] = await backend.pair();
      const user = { userId: 'eve', name: 'Eve' };
      const results = await Promise.all([a.presenceJoin('race', 'c1', user), b.presenceJoin('race', 'c2', user)]);
      expect(results.filter(Boolean)).toHaveLength(1);
    });
  });
}

describe('Redis adapter: crashed nodes', () => {
  it('ignores and cleans up presence left behind by a node that stopped heart-beating', async () => {
    const host = `mock-${randomUUID()}`;
    let now = 1_000_000;
    const clock = () => now;
    const make = async () => {
      const publisher = new RedisMock({ host }) as unknown as Redis;
      const adapter = new RedisAdapter({
        publisher,
        subscriber: publisher.duplicate(),
        keyPrefix: 'crash:',
        nodeHeartbeatMs: 60_000,
        nodeTtlMs: 15_000,
        clock,
      });
      await adapter.start();
      opened.push(adapter);
      return adapter;
    };
    const crashed = await make();
    const survivor = await make();
    await crashed.presenceJoin('lobby', 'c1', { userId: 'ghost', name: 'Ghost' });
    await survivor.presenceJoin('lobby', 'c2', { userId: 'alive', name: 'Alive' });
    expect((await survivor.presenceList('lobby')).map((member) => member.userId)).toEqual(['alive', 'ghost']);

    // Twenty seconds pass without a heartbeat from the crashed node (its timer is set to 60 s).
    // A node's own entries always count as live; the crashed node's are now past the 15 s TTL.
    now += 20_000;
    expect((await survivor.presenceList('lobby')).map((member) => member.userId)).toEqual(['alive']);
    // The stale entry was removed, and the ghost user can come back as a fresh join.
    expect(await survivor.presenceJoin('lobby', 'c3', { userId: 'ghost', name: 'Ghost' })).toBe(true);
  });
});
