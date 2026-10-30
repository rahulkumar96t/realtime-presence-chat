import { z } from 'zod';

/**
 * Wire protocol. Every frame is a JSON text frame with a protocol version `v`. Clients may also
 * negotiate the version through the `chat.v1` WebSocket subprotocol. Adding a version means
 * adding a schema and a branch in `parseClientMessage`; old clients keep working meanwhile.
 */
export const SUPPORTED_VERSIONS = [1] as const;
export const SUBPROTOCOL = 'chat.v1';

export const ROOM_NAME = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const Room = z.string().regex(ROOM_NAME, 'room names are 1-64 characters: a-z, 0-9, "_" and "-"');

export const MAX_TEXT_LENGTH = 2_000;

export const ClientMessageSchema = z.discriminatedUnion('type', [
  z.object({ v: z.literal(1), type: z.literal('join'), room: Room }),
  z.object({ v: z.literal(1), type: z.literal('leave'), room: Room }),
  z.object({
    v: z.literal(1),
    type: z.literal('message'),
    room: Room,
    text: z.string().trim().min(1).max(MAX_TEXT_LENGTH),
    clientMsgId: z.string().min(1).max(64).optional(),
  }),
  z.object({ v: z.literal(1), type: z.literal('typing'), room: Room, isTyping: z.boolean() }),
  z.object({ v: z.literal(1), type: z.literal('ping') }),
]);

export type ClientMessage = z.infer<typeof ClientMessageSchema>;

export interface Member {
  userId: string;
  name: string;
}

export interface ChatMessage {
  id: string;
  room: string;
  user: Member;
  text: string;
  sentAt: string;
  clientMsgId?: string;
}

export type ErrorCode =
  | 'bad_json'
  | 'unsupported_version'
  | 'invalid_message'
  | 'rate_limited'
  | 'not_in_room'
  | 'too_many_rooms'
  | 'internal_error';

export type ServerMessage =
  | {
      v: 1;
      type: 'welcome';
      connectionId: string;
      user: Member;
      protocolVersions: readonly number[];
      heartbeatIntervalMs: number;
      rateLimit: { capacity: number; refillPerSecond: number };
    }
  | { v: 1; type: 'joined'; room: string; members: Member[]; history: ChatMessage[] }
  | { v: 1; type: 'left'; room: string }
  | ({ v: 1; type: 'message' } & ChatMessage)
  | { v: 1; type: 'typing'; room: string; user: Member; isTyping: boolean }
  | { v: 1; type: 'presence'; room: string; event: 'join' | 'leave'; member: Member }
  | { v: 1; type: 'error'; code: ErrorCode; message: string; retryAfterMs?: number; request?: string }
  | { v: 1; type: 'pong' };

export type ParseResult = { ok: true; message: ClientMessage } | { ok: false; code: ErrorCode; message: string };

export function parseClientMessage(raw: string): ParseResult {
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    return { ok: false, code: 'bad_json', message: 'Frames must be JSON text.' };
  }
  if (typeof data !== 'object' || data === null || Array.isArray(data)) {
    return { ok: false, code: 'invalid_message', message: 'Frames must be JSON objects.' };
  }
  const version = (data as { v?: unknown }).v;
  if (!SUPPORTED_VERSIONS.includes(version as 1)) {
    return {
      ok: false,
      code: 'unsupported_version',
      message: `Protocol version ${JSON.stringify(version)} is not supported; use one of ${SUPPORTED_VERSIONS.join(', ')}.`,
    };
  }
  const parsed = ClientMessageSchema.safeParse(data);
  if (!parsed.success) {
    return { ok: false, code: 'invalid_message', message: z.prettifyError(parsed.error) };
  }
  return { ok: true, message: parsed.data };
}
