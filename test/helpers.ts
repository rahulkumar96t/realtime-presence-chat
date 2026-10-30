import { type ClientOptions, WebSocket } from 'ws';
import { mintToken, type MintOptions } from '../src/auth.js';
import type { ClusterAdapter } from '../src/cluster/adapter.js';
import { InMemoryAdapter, InMemoryBroker } from '../src/cluster/memory.js';
import { ChatServer, type ChatServerOptions } from '../src/server.js';

export const SECRET = 'test-secret-that-is-long-enough-0123456789';

/** Loosely typed server frame, as a client would see it. */
export type Frame = { v: number; type: string } & Record<string, unknown>;

export interface TestNode {
  server: ChatServer;
  port: number;
  wsUrl: string;
  httpUrl: string;
  close(): Promise<void>;
}

export async function startNode(
  options: Partial<ChatServerOptions> & { broker?: InMemoryBroker } = {},
): Promise<TestNode> {
  const { broker, ...rest } = options;
  const adapter: ClusterAdapter = rest.adapter ?? new InMemoryAdapter(broker ?? new InMemoryBroker());
  const server = new ChatServer({
    jwtSecret: SECRET,
    collectProcessMetrics: false,
    shutdownGraceMs: 1_000,
    ...rest,
    adapter,
  });
  const port = await server.listen();
  return {
    server,
    port,
    wsUrl: `ws://127.0.0.1:${port}/ws`,
    httpUrl: `http://127.0.0.1:${port}`,
    close: () => server.close(),
  };
}

export function token(userId: string, name?: string, extra: Partial<MintOptions> = {}): Promise<string> {
  return mintToken(SECRET, { userId, name, ...extra });
}

interface Waiter {
  predicate: (frame: Frame) => boolean;
  resolve: (frame: Frame) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

/** A real `ws` client that records frames and lets tests await specific ones. */
export class TestClient {
  /** Frames received and not yet taken by `take`. */
  readonly inbox: Frame[] = [];
  readonly closed: Promise<{ code: number; reason: string }>;
  private readonly waiters: Waiter[] = [];

  private constructor(readonly socket: WebSocket) {
    socket.on('message', (data) => {
      const frame = JSON.parse(data.toString()) as Frame;
      const index = this.waiters.findIndex((waiter) => waiter.predicate(frame));
      if (index >= 0) {
        const [waiter] = this.waiters.splice(index, 1) as [Waiter];
        clearTimeout(waiter.timer);
        waiter.resolve(frame);
      } else {
        this.inbox.push(frame);
      }
    });
    this.closed = new Promise((resolve) =>
      socket.on('close', (code, reason) => resolve({ code, reason: reason.toString() })),
    );
  }

  static async connect(
    url: string,
    bearer: string | undefined,
    options: ClientOptions & { protocols?: string[]; via?: 'header' | 'query' } = {},
  ): Promise<TestClient> {
    const { protocols, via = 'header', ...clientOptions } = options;
    const target = new URL(url);
    const headers: Record<string, string> = {};
    if (bearer && via === 'query') target.searchParams.set('access_token', bearer);
    if (bearer && via === 'header') headers.authorization = `Bearer ${bearer}`;
    const socket = new WebSocket(target, protocols, { headers, ...clientOptions });
    const client = new TestClient(socket);
    await new Promise<void>((resolve, reject) => {
      socket.once('open', () => resolve());
      socket.once('error', reject);
    });
    return client;
  }

  /** Connects and waits for the welcome frame. */
  static async ready(url: string, bearer: string, options: ClientOptions = {}): Promise<TestClient> {
    const client = await TestClient.connect(url, bearer, options);
    await client.take((frame) => frame.type === 'welcome');
    return client;
  }

  send(frame: Record<string, unknown>): void {
    this.socket.send(JSON.stringify({ v: 1, ...frame }));
  }

  /** Resolves with the first frame (already received or future) matching the predicate. */
  take(predicate: (frame: Frame) => boolean, timeoutMs = 3_000): Promise<Frame> {
    const index = this.inbox.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.inbox.splice(index, 1)[0] as Frame);
    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        predicate,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          reject(new Error(`timed out waiting for a frame; inbox: ${JSON.stringify(this.inbox.slice(-5))}`));
        }, timeoutMs),
      };
      this.waiters.push(waiter);
    });
  }

  takeType(type: string, extra: (frame: Frame) => boolean = () => true, timeoutMs?: number): Promise<Frame> {
    return this.take((frame) => frame.type === type && extra(frame), timeoutMs);
  }

  /** Joins a room and returns the snapshot. */
  async join(room: string): Promise<Frame> {
    this.send({ type: 'join', room });
    return this.takeType('joined', (frame) => frame.room === room);
  }

  /** Asserts that no matching frame arrives within `ms`. */
  async expectNone(predicate: (frame: Frame) => boolean, ms = 200): Promise<void> {
    const found = await this.take(predicate, ms).then(
      (frame) => frame,
      () => undefined,
    );
    if (found) throw new Error(`unexpected frame: ${JSON.stringify(found)}`);
  }

  close(): Promise<{ code: number; reason: string }> {
    if (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING) {
      this.socket.close(1000);
    }
    return this.closed;
  }
}

/** Attempts an upgrade that the server is expected to refuse; resolves with the HTTP status. */
export function rejectedStatus(url: string, headers: Record<string, string> = {}, protocols?: string[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url, protocols, { headers });
    socket.on('unexpected-response', (_request, response) => {
      resolve(response.statusCode ?? 0);
      socket.terminate();
    });
    socket.on('open', () => reject(new Error('upgrade unexpectedly succeeded')));
    socket.on('error', () => undefined);
  });
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function metricValue(httpUrl: string, line: RegExp): Promise<number> {
  const text = await (await fetch(`${httpUrl}/metrics`)).text();
  const match = text.split('\n').find((candidate) => line.test(candidate));
  return match ? Number(match.split(' ').pop()) : 0;
}
