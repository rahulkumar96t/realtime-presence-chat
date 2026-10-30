import type { ChatMessage, Member } from '../protocol.js';

/** Events that fan out to every member of a room, on every node. */
export type RoomEvent =
  | { kind: 'message'; message: ChatMessage }
  | { kind: 'typing'; room: string; user: Member; isTyping: boolean; connectionId: string }
  | { kind: 'presence'; room: string; event: 'join' | 'leave'; member: Member; connectionId: string };

export type RoomEventHandler = (event: RoomEvent) => void;

/**
 * Everything that must be shared between server nodes. A node only keeps its own sockets;
 * room fan-out, history and presence go through the adapter, so any number of nodes can serve
 * the same rooms.
 *
 * Presence is tracked per connection (`connectionId`) and reported per user, so a user with two
 * tabs is "present" until the last tab leaves. `presenceJoin` / `presenceLeave` return whether
 * the user's presence changed, which decides whether a presence event is broadcast.
 */
export interface ClusterAdapter {
  readonly nodeId: string;
  readonly historySize: number;

  publish(room: string, event: RoomEvent): Promise<void>;
  /** At most one handler per room per node; the node fans out to its local sockets. */
  subscribe(room: string, handler: RoomEventHandler): Promise<void>;
  unsubscribe(room: string): Promise<void>;

  appendHistory(message: ChatMessage): Promise<void>;
  /** The most recent messages, oldest first. */
  history(room: string): Promise<ChatMessage[]>;

  /** Returns true when the user was not present in the room before this connection joined. */
  presenceJoin(room: string, connectionId: string, member: Member): Promise<boolean>;
  /** Returns true when the user has no connection left in the room. */
  presenceLeave(room: string, connectionId: string, userId: string): Promise<boolean>;
  presenceList(room: string): Promise<Member[]>;

  close(): Promise<void>;
}

/** Collapses per-connection entries into one member per user, ordered by name. */
export function uniqueMembers(entries: Iterable<Member>): Member[] {
  const byUser = new Map<string, Member>();
  for (const member of entries) {
    if (!byUser.has(member.userId)) byUser.set(member.userId, member);
  }
  return [...byUser.values()].sort((a, b) => a.name.localeCompare(b.name) || a.userId.localeCompare(b.userId));
}
