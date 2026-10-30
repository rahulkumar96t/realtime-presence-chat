import { randomUUID } from 'node:crypto';
import type { ChatMessage, Member } from '../protocol.js';
import { RingBuffer } from '../ringBuffer.js';
import { type ClusterAdapter, type RoomEvent, type RoomEventHandler, uniqueMembers } from './adapter.js';

/**
 * Shared state for in-process "nodes": one broker stands in for Redis. Several ChatServer
 * instances in one process (as in the tests) cluster by sharing a broker; a single-node
 * deployment simply has one.
 *
 * Deliveries are asynchronous and structured-cloned, like a real network hop, so code that
 * accidentally depends on synchronous or shared-object delivery fails here too.
 */
export class InMemoryBroker {
  private readonly subscribers = new Map<string, Map<string, RoomEventHandler>>();
  private readonly histories = new Map<string, RingBuffer<ChatMessage>>();
  private readonly presence = new Map<string, Map<string, Member>>();

  constructor(readonly historySize = 50) {}

  publish(room: string, event: RoomEvent): void {
    const handlers = [...(this.subscribers.get(room)?.values() ?? [])];
    for (const handler of handlers) {
      const copy = structuredClone(event);
      setImmediate(() => handler(copy));
    }
  }

  subscribe(nodeId: string, room: string, handler: RoomEventHandler): void {
    let handlers = this.subscribers.get(room);
    if (!handlers) {
      handlers = new Map();
      this.subscribers.set(room, handlers);
    }
    handlers.set(nodeId, handler);
  }

  unsubscribe(nodeId: string, room: string): void {
    const handlers = this.subscribers.get(room);
    handlers?.delete(nodeId);
    if (handlers?.size === 0) this.subscribers.delete(room);
  }

  appendHistory(message: ChatMessage): void {
    let buffer = this.histories.get(message.room);
    if (!buffer) {
      buffer = new RingBuffer(this.historySize);
      this.histories.set(message.room, buffer);
    }
    buffer.push(structuredClone(message));
  }

  history(room: string): ChatMessage[] {
    return structuredClone(this.histories.get(room)?.toArray() ?? []);
  }

  presenceJoin(room: string, key: string, member: Member): boolean {
    let entries = this.presence.get(room);
    if (!entries) {
      entries = new Map();
      this.presence.set(room, entries);
    }
    const wasPresent = [...entries.values()].some((entry) => entry.userId === member.userId);
    entries.set(key, { ...member });
    return !wasPresent;
  }

  presenceLeave(room: string, key: string, userId: string): boolean {
    const entries = this.presence.get(room);
    if (!entries) return false;
    const removed = entries.delete(key);
    const stillPresent = [...entries.values()].some((entry) => entry.userId === userId);
    if (entries.size === 0) this.presence.delete(room);
    return removed && !stillPresent;
  }

  presenceList(room: string): Member[] {
    return uniqueMembers(this.presence.get(room)?.values() ?? []);
  }

  /** Drops a node's subscriptions (used when a node shuts down). */
  removeNode(nodeId: string): void {
    for (const room of [...this.subscribers.keys()]) this.unsubscribe(nodeId, room);
  }
}

export class InMemoryAdapter implements ClusterAdapter {
  readonly nodeId: string;

  constructor(
    private readonly broker: InMemoryBroker = new InMemoryBroker(),
    nodeId: string = randomUUID(),
  ) {
    this.nodeId = nodeId;
  }

  get historySize(): number {
    return this.broker.historySize;
  }

  async publish(room: string, event: RoomEvent): Promise<void> {
    this.broker.publish(room, event);
  }

  async subscribe(room: string, handler: RoomEventHandler): Promise<void> {
    this.broker.subscribe(this.nodeId, room, handler);
  }

  async unsubscribe(room: string): Promise<void> {
    this.broker.unsubscribe(this.nodeId, room);
  }

  async appendHistory(message: ChatMessage): Promise<void> {
    this.broker.appendHistory(message);
  }

  async history(room: string): Promise<ChatMessage[]> {
    return this.broker.history(room);
  }

  async presenceJoin(room: string, connectionId: string, member: Member): Promise<boolean> {
    return this.broker.presenceJoin(room, this.key(connectionId), member);
  }

  async presenceLeave(room: string, connectionId: string, userId: string): Promise<boolean> {
    return this.broker.presenceLeave(room, this.key(connectionId), userId);
  }

  async presenceList(room: string): Promise<Member[]> {
    return this.broker.presenceList(room);
  }

  async close(): Promise<void> {
    this.broker.removeNode(this.nodeId);
  }

  private key(connectionId: string): string {
    return `${this.nodeId}/${connectionId}`;
  }
}
