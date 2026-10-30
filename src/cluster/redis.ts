import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import type { ChatMessage, Member } from '../protocol.js';
import { type ClusterAdapter, type RoomEvent, type RoomEventHandler, uniqueMembers } from './adapter.js';

export interface RedisAdapterOptions {
  /** Connection for commands and PUBLISH. */
  publisher: Redis;
  /** Dedicated connection in subscriber mode (a Redis connection cannot do both). */
  subscriber: Redis;
  nodeId?: string;
  keyPrefix?: string;
  historySize?: number;
  /** History lists expire this long after the last message. */
  historyTtlMs?: number;
  /** How often this node refreshes its liveness entry. */
  nodeHeartbeatMs?: number;
  /** Presence entries of nodes silent for longer than this are ignored and cleaned up. */
  nodeTtlMs?: number;
  clock?: () => number;
}

type MultiResult = Array<[Error | null, unknown]> | null;

/**
 * Redis-backed cluster state:
 * - fan-out:  PUBLISH / SUBSCRIBE on `<prefix>room:<room>`
 * - history:  a capped list per room (RPUSH + LTRIM), the Redis form of a ring buffer
 * - presence: a hash per room, field `<nodeId>/<connectionId>` -> member JSON
 * - liveness: a hash `<prefix>nodes`, nodeId -> last heartbeat (ms)
 *
 * Presence changes run in MULTI/EXEC so "was this user present before?" is answered from the
 * same atomic snapshot the write applies to; two nodes adding the same user concurrently produce
 * exactly one join event. Entries of nodes that stopped heartbeating (crashed) are ignored and
 * removed lazily, so a crashed node cannot leave members "online" forever.
 */
export class RedisAdapter implements ClusterAdapter {
  readonly nodeId: string;
  readonly historySize: number;
  private readonly publisher: Redis;
  private readonly subscriber: Redis;
  private readonly prefix: string;
  private readonly historyTtlMs: number;
  private readonly nodeHeartbeatMs: number;
  private readonly nodeTtlMs: number;
  private readonly clock: () => number;
  private readonly handlers = new Map<string, RoomEventHandler>();
  private heartbeat: NodeJS.Timeout | undefined;

  constructor(options: RedisAdapterOptions) {
    this.publisher = options.publisher;
    this.subscriber = options.subscriber;
    this.nodeId = options.nodeId ?? randomUUID();
    this.prefix = options.keyPrefix ?? 'chat:';
    this.historySize = options.historySize ?? 50;
    this.historyTtlMs = options.historyTtlMs ?? 7 * 24 * 3600 * 1000;
    this.nodeHeartbeatMs = options.nodeHeartbeatMs ?? 5_000;
    this.nodeTtlMs = options.nodeTtlMs ?? 15_000;
    this.clock = options.clock ?? Date.now;

    this.subscriber.on('message', (channel: string, payload: string) => {
      const room = channel.slice(this.channel('').length);
      const handler = this.handlers.get(room);
      if (!handler) return;
      let event: RoomEvent;
      try {
        event = JSON.parse(payload) as RoomEvent;
      } catch {
        return;
      }
      handler(event);
    });
  }

  /** Opens both connections from a URL and starts the liveness heartbeat. */
  static async connect(url: string, options: Omit<RedisAdapterOptions, 'publisher' | 'subscriber'> = {}) {
    const publisher = new Redis(url, { maxRetriesPerRequest: 3 });
    const subscriber = publisher.duplicate();
    const adapter = new RedisAdapter({ ...options, publisher, subscriber });
    await adapter.start();
    return adapter;
  }

  async start(): Promise<void> {
    await this.beat();
    this.heartbeat = setInterval(() => {
      this.beat().catch(() => undefined);
    }, this.nodeHeartbeatMs);
    this.heartbeat.unref();
  }

  async publish(room: string, event: RoomEvent): Promise<void> {
    await this.publisher.publish(this.channel(room), JSON.stringify(event));
  }

  async subscribe(room: string, handler: RoomEventHandler): Promise<void> {
    this.handlers.set(room, handler);
    await this.subscriber.subscribe(this.channel(room));
  }

  async unsubscribe(room: string): Promise<void> {
    this.handlers.delete(room);
    await this.subscriber.unsubscribe(this.channel(room));
  }

  async appendHistory(message: ChatMessage): Promise<void> {
    const key = this.key('history', message.room);
    check(
      await this.publisher
        .multi()
        .rpush(key, JSON.stringify(message))
        .ltrim(key, -this.historySize, -1)
        .pexpire(key, this.historyTtlMs)
        .exec(),
    );
  }

  async history(room: string): Promise<ChatMessage[]> {
    const items = await this.publisher.lrange(this.key('history', room), -this.historySize, -1);
    return items.map((item) => JSON.parse(item) as ChatMessage);
  }

  async presenceJoin(room: string, connectionId: string, member: Member): Promise<boolean> {
    const key = this.key('presence', room);
    const results = check(
      await this.publisher
        .multi()
        .hgetall(key)
        .hgetall(this.nodesKey())
        .hset(key, this.field(connectionId), JSON.stringify(member))
        .exec(),
    );
    const before = this.liveEntries(results[0] as Record<string, string>, results[1] as Record<string, string>);
    return !before.some(([, entry]) => entry.userId === member.userId);
  }

  async presenceLeave(room: string, connectionId: string, userId: string): Promise<boolean> {
    const key = this.key('presence', room);
    const results = check(
      await this.publisher.multi().hdel(key, this.field(connectionId)).hgetall(key).hgetall(this.nodesKey()).exec(),
    );
    const removed = Number(results[0]) === 1;
    const after = this.liveEntries(results[1] as Record<string, string>, results[2] as Record<string, string>);
    return removed && !after.some(([, entry]) => entry.userId === userId);
  }

  async presenceList(room: string): Promise<Member[]> {
    const key = this.key('presence', room);
    const results = check(await this.publisher.multi().hgetall(key).hgetall(this.nodesKey()).exec());
    const entries = results[0] as Record<string, string>;
    const live = this.liveEntries(entries, results[1] as Record<string, string>);
    const dead = Object.keys(entries).filter((field) => !live.some(([liveField]) => liveField === field));
    if (dead.length > 0) {
      // Lazy clean-up of members left behind by crashed nodes.
      await this.publisher.hdel(key, ...dead).catch(() => undefined);
    }
    return uniqueMembers(live.map(([, member]) => member));
  }

  async close(): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.handlers.clear();
    await this.publisher.hdel(this.nodesKey(), this.nodeId).catch(() => undefined);
    await Promise.allSettled([this.subscriber.quit(), this.publisher.quit()]);
  }

  private async beat(): Promise<void> {
    await this.publisher.hset(this.nodesKey(), this.nodeId, String(this.clock()));
  }

  /** Presence entries whose node has heart-beaten recently (this node always counts as live). */
  private liveEntries(entries: Record<string, string>, nodes: Record<string, string>): Array<[string, Member]> {
    const cutoff = this.clock() - this.nodeTtlMs;
    const live: Array<[string, Member]> = [];
    for (const [field, value] of Object.entries(entries ?? {})) {
      const nodeId = field.slice(0, field.indexOf('/'));
      const lastSeen = Number(nodes?.[nodeId] ?? 0);
      if (nodeId !== this.nodeId && lastSeen < cutoff) continue;
      try {
        live.push([field, JSON.parse(value) as Member]);
      } catch {
        // Ignore entries that are not ours to understand.
      }
    }
    return live;
  }

  private channel(room: string): string {
    return `${this.prefix}room:${room}`;
  }

  private key(kind: 'history' | 'presence', room: string): string {
    return `${this.prefix}${kind}:${room}`;
  }

  private nodesKey(): string {
    return `${this.prefix}nodes`;
  }

  private field(connectionId: string): string {
    return `${this.nodeId}/${connectionId}`;
  }
}

/** Unwraps MULTI/EXEC results, throwing on an aborted transaction or a failed command. */
function check(results: MultiResult): unknown[] {
  if (!results) throw new Error('Redis transaction was aborted');
  return results.map(([error, value]) => {
    if (error) throw error;
    return value;
  });
}
