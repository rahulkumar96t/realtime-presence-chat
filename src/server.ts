import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type Server, type ServerResponse, STATUS_CODES } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Duplex } from 'node:stream';
import { type RawData, WebSocket, WebSocketServer } from 'ws';
import { AuthError, type Identity, mintToken, tokenFromRequest, TokenVerifier } from './auth.js';
import type { ClusterAdapter, RoomEvent } from './cluster/adapter.js';
import { type BackpressurePolicy, ClientConnection, type Delivery } from './connection.js';
import { type Logger, silentLogger } from './logger.js';
import { ChatMetrics, type DisconnectReason } from './metrics.js';
import {
  type ChatMessage,
  type ClientMessage,
  type ErrorCode,
  parseClientMessage,
  type ServerMessage,
  SUBPROTOCOL,
  SUPPORTED_VERSIONS,
} from './protocol.js';

export interface ChatServerOptions {
  adapter: ClusterAdapter;
  jwtSecret: string;
  jwtIssuer?: string;
  jwtAudience?: string;
  port?: number;
  host?: string;
  heartbeatIntervalMs?: number;
  rateLimit?: { capacity: number; refillPerSecond: number; maxViolations: number };
  backpressure?: BackpressurePolicy;
  maxPayloadBytes?: number;
  maxRoomsPerConnection?: number;
  shutdownGraceMs?: number;
  /** Serves GET /dev/token?name=... for the demo page. Never enable in production. */
  devTokens?: boolean;
  logger?: Logger;
  collectProcessMetrics?: boolean;
}

const DEFAULTS = {
  heartbeatIntervalMs: 30_000,
  rateLimit: { capacity: 10, refillPerSecond: 5, maxViolations: 20 },
  backpressure: { softLimitBytes: 256 * 1024, hardLimitBytes: 1024 * 1024 },
  maxPayloadBytes: 16 * 1024,
  maxRoomsPerConnection: 20,
  shutdownGraceMs: 5_000,
};

/** Application close codes (4000-4999 are reserved for applications). */
export const CloseCodes = {
  goingAway: 1001,
  policyViolation: 1008,
  tryAgainLater: 1013,
  tokenExpired: 4001,
} as const;

const DEMO_PAGE = new URL('../public/index.html', import.meta.url);

export class ChatServer {
  readonly metrics: ChatMetrics;
  private readonly adapter: ClusterAdapter;
  private readonly verifier: TokenVerifier;
  private readonly logger: Logger;
  private readonly http: Server;
  private readonly wss: WebSocketServer;
  private readonly connections = new Map<string, ClientConnection>();
  private readonly localRooms = new Map<string, Set<ClientConnection>>();
  private readonly subscriptions = new Map<string, Promise<void>>();
  private readonly settings: typeof DEFAULTS;
  private heartbeat: NodeJS.Timeout | undefined;
  private draining = false;
  private closing: Promise<void> | undefined;
  private demoPage: Buffer | undefined;

  constructor(private readonly options: ChatServerOptions) {
    this.adapter = options.adapter;
    this.logger = options.logger ?? silentLogger;
    this.metrics = new ChatMetrics({ collectProcessMetrics: options.collectProcessMetrics });
    this.verifier = new TokenVerifier(options.jwtSecret, { issuer: options.jwtIssuer, audience: options.jwtAudience });
    this.settings = {
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? DEFAULTS.heartbeatIntervalMs,
      rateLimit: options.rateLimit ?? DEFAULTS.rateLimit,
      backpressure: options.backpressure ?? DEFAULTS.backpressure,
      maxPayloadBytes: options.maxPayloadBytes ?? DEFAULTS.maxPayloadBytes,
      maxRoomsPerConnection: options.maxRoomsPerConnection ?? DEFAULTS.maxRoomsPerConnection,
      shutdownGraceMs: options.shutdownGraceMs ?? DEFAULTS.shutdownGraceMs,
    };

    this.http = createServer((request, response) => {
      this.onRequest(request, response).catch((error: unknown) => {
        this.logger.error('http handler failed', { error });
        if (!response.headersSent) response.writeHead(500).end();
      });
    });
    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: this.settings.maxPayloadBytes,
      // Clients that offer subprotocols must offer ours; clients that offer none are accepted.
      handleProtocols: (protocols) => (protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false),
    });
    this.http.on('upgrade', (request, socket, head) => this.onUpgrade(request, socket, head));
  }

  /** Starts listening; resolves with the bound port (useful with port 0). */
  async listen(): Promise<number> {
    await new Promise<void>((resolve, reject) => {
      this.http.once('error', reject);
      this.http.listen(this.options.port ?? 0, this.options.host ?? '127.0.0.1', () => {
        this.http.off('error', reject);
        resolve();
      });
    });
    this.heartbeat = setInterval(() => this.sweep(), this.settings.heartbeatIntervalMs);
    this.heartbeat.unref();
    const { port } = this.http.address() as AddressInfo;
    this.logger.info('listening', { port, nodeId: this.adapter.nodeId });
    return port;
  }

  get connectionCount(): number {
    return this.connections.size;
  }

  get nodeId(): string {
    return this.adapter.nodeId;
  }

  /**
   * Graceful shutdown: keep listening but answer /readyz and new upgrades with 503 (so a load
   * balancer stops routing here), close every socket with 1001 so clients reconnect elsewhere,
   * publish their presence departures, then release the adapter and stop listening. Sockets
   * that do not finish the closing handshake within the grace period are terminated.
   */
  close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  // --- HTTP ----------------------------------------------------------------------------------

  private async onRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? '/', 'http://localhost');
    const json = (status: number, body: unknown) =>
      response.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(body));

    if (request.method !== 'GET') return void json(405, { error: 'method_not_allowed' });
    switch (url.pathname) {
      case '/healthz':
        return void json(200, { status: 'ok' });
      case '/readyz':
        return void json(this.draining ? 503 : 200, { status: this.draining ? 'draining' : 'ready' });
      case '/metrics': {
        const body = await this.metrics.registry.metrics();
        return void response.writeHead(200, { 'content-type': this.metrics.registry.contentType }).end(body);
      }
      case '/dev/token': {
        if (!this.options.devTokens) return void json(404, { error: 'not_found' });
        const name = (url.searchParams.get('name') ?? 'guest').slice(0, 32) || 'guest';
        const token = await mintToken(this.options.jwtSecret, {
          userId: `${name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-${randomUUID().slice(0, 8)}`,
          name,
          ttlSeconds: 3600,
          issuer: this.options.jwtIssuer,
          audience: this.options.jwtAudience,
        });
        return void json(200, { token });
      }
      case '/':
      case '/index.html':
        this.demoPage ??= readFileSync(DEMO_PAGE);
        return void response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }).end(this.demoPage);
      default:
        return void json(404, { error: 'not_found' });
    }
  }

  private onUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    socket.on('error', (error) => this.logger.debug('upgrade socket error', { error }));
    const url = new URL(request.url ?? '/', 'http://localhost');
    if (url.pathname !== '/ws') return rejectUpgrade(socket, 404);
    if (this.draining) return rejectUpgrade(socket, 503);

    this.verifier.verify(tokenFromRequest(request)).then(
      (identity) => {
        if (this.draining) return rejectUpgrade(socket, 503);
        this.wss.handleUpgrade(request, socket, head, (ws) => this.onConnection(ws, identity));
      },
      (error: unknown) => {
        const reason = error instanceof AuthError ? error.reason : 'malformed';
        this.metrics.authFailures.inc({ reason });
        this.logger.info('upgrade rejected', { reason });
        rejectUpgrade(socket, 401, { 'www-authenticate': `Bearer error="invalid_token", error_description="${reason}"` });
      },
    );
  }

  // --- connections ---------------------------------------------------------------------------

  private onConnection(socket: WebSocket, identity: Identity): void {
    const connection = new ClientConnection(
      randomUUID(),
      { userId: identity.userId, name: identity.name },
      socket,
      {
        backpressure: this.settings.backpressure,
        rateLimit: this.settings.rateLimit,
        metrics: this.metrics,
        onSlowConsumer: (slow) => this.disconnect(slow, 'slow_consumer'),
      },
    );
    this.connections.set(connection.id, connection);
    this.metrics.connections.set(this.connections.size);
    this.metrics.connectionsOpened.inc();

    const expiry = setTimeout(
      () => {
        connection.closeReason = 'token_expired';
        socket.close(CloseCodes.tokenExpired, 'token expired');
      },
      Math.min(Math.max(identity.expiresAt - Date.now(), 0), 2 ** 31 - 1),
    );

    socket.on('pong', () => {
      connection.isAlive = true;
    });
    socket.on('message', (data, isBinary) => this.onFrame(connection, data, isBinary));
    socket.on('error', (error) => this.logger.debug('socket error', { connectionId: connection.id, error }));
    socket.on('close', (code) => {
      clearTimeout(expiry);
      this.connections.delete(connection.id);
      this.metrics.connections.set(this.connections.size);
      const reason = connection.closeReason ?? 'client';
      this.metrics.disconnects.inc({ reason });
      this.logger.info('disconnected', { connectionId: connection.id, userId: connection.user.userId, code, reason });
      void connection.enqueue(() => this.leaveAll(connection));
    });

    connection.send({
      v: 1,
      type: 'welcome',
      connectionId: connection.id,
      user: connection.user,
      protocolVersions: SUPPORTED_VERSIONS,
      heartbeatIntervalMs: this.settings.heartbeatIntervalMs,
      rateLimit: {
        capacity: this.settings.rateLimit.capacity,
        refillPerSecond: this.settings.rateLimit.refillPerSecond,
      },
    });
    this.logger.info('connected', { connectionId: connection.id, userId: connection.user.userId });
  }

  private onFrame(connection: ClientConnection, data: RawData, isBinary: boolean): void {
    if (!connection.bucket.tryRemove()) {
      connection.violations += 1;
      this.metrics.framesDropped.inc({ reason: 'rate_limited' });
      if (connection.violations >= this.settings.rateLimit.maxViolations) {
        this.disconnect(connection, 'rate_limit');
        return;
      }
      this.sendError(connection, 'rate_limited', 'Too many frames; slow down.', undefined, connection.bucket.msUntilAvailable());
      return;
    }
    connection.violations = 0;

    if (isBinary) {
      this.metrics.framesReceived.inc({ type: 'invalid' });
      return this.sendError(connection, 'invalid_message', 'Binary frames are not supported.');
    }
    const parsed = parseClientMessage(data.toString());
    if (!parsed.ok) {
      this.metrics.framesReceived.inc({ type: 'invalid' });
      return this.sendError(connection, parsed.code, parsed.message);
    }
    const message = parsed.message;
    this.metrics.framesReceived.inc({ type: message.type });

    void connection.enqueue(async () => {
      const timer = this.metrics.handleSeconds.startTimer({ type: message.type });
      try {
        await this.dispatch(connection, message);
      } catch (error) {
        this.logger.error('handler failed', { connectionId: connection.id, type: message.type, error });
        this.sendError(connection, 'internal_error', 'The request could not be processed.', message.type);
      } finally {
        timer();
      }
    });
  }

  private async dispatch(connection: ClientConnection, message: ClientMessage): Promise<void> {
    switch (message.type) {
      case 'join':
        return this.join(connection, message.room);
      case 'leave':
        if (connection.rooms.has(message.room)) await this.leave(connection, message.room);
        connection.send({ v: 1, type: 'left', room: message.room });
        return;
      case 'message': {
        if (!connection.rooms.has(message.room)) return this.notInRoom(connection, message);
        const chat: ChatMessage = {
          id: randomUUID(),
          room: message.room,
          user: connection.user,
          text: message.text,
          sentAt: new Date().toISOString(),
          ...(message.clientMsgId ? { clientMsgId: message.clientMsgId } : {}),
        };
        await this.adapter.appendHistory(chat);
        await this.adapter.publish(message.room, { kind: 'message', message: chat });
        return;
      }
      case 'typing':
        if (!connection.rooms.has(message.room)) return this.notInRoom(connection, message);
        await this.adapter.publish(message.room, {
          kind: 'typing',
          room: message.room,
          user: connection.user,
          isTyping: message.isTyping,
          connectionId: connection.id,
        });
        return;
      case 'ping':
        connection.send({ v: 1, type: 'pong' });
        return;
    }
  }

  // --- rooms ---------------------------------------------------------------------------------

  /**
   * Join protocol, ordered so a late joiner neither misses nor duplicates messages:
   * 1. become a local member (live events start buffering in `syncing`),
   * 2. make sure this node is subscribed to the room,
   * 3. register presence, read history and the member list,
   * 4. send the snapshot, then flush buffered events that are not already in the history.
   */
  private async join(connection: ClientConnection, room: string): Promise<void> {
    if (!connection.rooms.has(room) && connection.rooms.size >= this.settings.maxRoomsPerConnection) {
      return this.sendError(
        connection,
        'too_many_rooms',
        `A connection may join at most ${this.settings.maxRoomsPerConnection} rooms.`,
        'join',
      );
    }
    const firstJoin = !connection.rooms.has(room);
    connection.rooms.add(room);
    connection.syncing.set(room, []);
    try {
      await this.addLocalMember(room, connection);
      const newlyPresent = firstJoin
        ? await this.adapter.presenceJoin(room, connection.id, connection.user)
        : false;
      const [history, members] = await Promise.all([this.adapter.history(room), this.adapter.presenceList(room)]);

      connection.send({ v: 1, type: 'joined', room, members, history });
      const buffered = connection.syncing.get(room) ?? [];
      connection.syncing.delete(room);
      const seen = new Set(history.map((item) => item.id));
      for (const item of buffered) {
        if (item.frame.type === 'message' && seen.has(item.frame.id)) continue;
        connection.send(item.frame, item.delivery);
      }

      if (newlyPresent) {
        await this.adapter.publish(room, {
          kind: 'presence',
          room,
          event: 'join',
          member: connection.user,
          connectionId: connection.id,
        });
      }
    } catch (error) {
      connection.syncing.delete(room);
      throw error;
    }
  }

  private async leave(connection: ClientConnection, room: string): Promise<void> {
    connection.rooms.delete(room);
    connection.syncing.delete(room);
    await this.removeLocalMember(room, connection);
    const gone = await this.adapter.presenceLeave(room, connection.id, connection.user.userId);
    if (gone) {
      await this.adapter.publish(room, {
        kind: 'presence',
        room,
        event: 'leave',
        member: connection.user,
        connectionId: connection.id,
      });
    }
  }

  private async leaveAll(connection: ClientConnection): Promise<void> {
    for (const room of [...connection.rooms]) {
      try {
        await this.leave(connection, room);
      } catch (error) {
        this.logger.error('leave failed', { connectionId: connection.id, room, error });
      }
    }
  }

  private async addLocalMember(room: string, connection: ClientConnection): Promise<void> {
    let members = this.localRooms.get(room);
    if (!members) {
      members = new Set();
      this.localRooms.set(room, members);
      this.metrics.localRooms.set(this.localRooms.size);
    }
    members.add(connection);
    let subscription = this.subscriptions.get(room);
    if (!subscription) {
      subscription = this.adapter.subscribe(room, (event) => this.fanOut(room, event));
      this.subscriptions.set(room, subscription);
      subscription.catch(() => this.subscriptions.delete(room));
    }
    await subscription;
  }

  private async removeLocalMember(room: string, connection: ClientConnection): Promise<void> {
    const members = this.localRooms.get(room);
    if (!members) return;
    members.delete(connection);
    if (members.size > 0) return;
    this.localRooms.delete(room);
    this.subscriptions.delete(room);
    this.metrics.localRooms.set(this.localRooms.size);
    await this.adapter.unsubscribe(room);
  }

  /** Delivers an event from the adapter to this node's members of the room. */
  private fanOut(room: string, event: RoomEvent): void {
    const members = this.localRooms.get(room);
    if (!members) return;
    const { frame, delivery, origin } = toFrame(event);
    for (const connection of members) {
      // Typing and presence are about the originating connection; it does not need them back.
      if (origin === connection.id) continue;
      connection.deliver(room, frame, delivery);
    }
  }

  // --- liveness and policy -------------------------------------------------------------------

  /** Heartbeat: terminate sockets that did not answer the previous ping, ping the rest. */
  private sweep(): void {
    for (const connection of this.connections.values()) {
      if (!connection.isAlive) {
        connection.closeReason = 'heartbeat_timeout';
        connection.socket.terminate();
        continue;
      }
      connection.isAlive = false;
      try {
        connection.socket.ping();
      } catch {
        connection.socket.terminate();
      }
    }
  }

  private disconnect(connection: ClientConnection, reason: DisconnectReason): void {
    connection.closeReason = reason;
    if (reason === 'rate_limit') {
      connection.socket.close(CloseCodes.policyViolation, 'rate limit exceeded');
      return;
    }
    // A slow consumer's buffer is full, so a close frame would never reach it: drop the socket.
    connection.socket.terminate();
  }

  private notInRoom(connection: ClientConnection, message: ClientMessage & { room: string }): void {
    this.sendError(connection, 'not_in_room', `Join "${message.room}" before sending to it.`, message.type);
  }

  private sendError(
    connection: ClientConnection,
    code: ErrorCode,
    message: string,
    request?: string,
    retryAfterMs?: number,
  ): void {
    connection.send({
      v: 1,
      type: 'error',
      code,
      message,
      ...(request ? { request } : {}),
      ...(retryAfterMs !== undefined ? { retryAfterMs } : {}),
    });
  }

  private async shutdown(): Promise<void> {
    this.draining = true;
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.logger.info('shutting down', { connections: this.connections.size });

    const open = [...this.connections.values()];
    for (const connection of open) {
      connection.closeReason = 'shutdown';
      connection.socket.close(CloseCodes.goingAway, 'server shutting down');
    }
    const closed = Promise.all(
      open.map((connection) =>
        connection.socket.readyState === WebSocket.CLOSED
          ? Promise.resolve()
          : new Promise<void>((resolve) => connection.socket.once('close', () => resolve())),
      ),
    );
    const grace = new Promise<'timeout'>((resolve) =>
      setTimeout(() => resolve('timeout'), this.settings.shutdownGraceMs).unref(),
    );
    if ((await Promise.race([closed, grace])) === 'timeout') {
      for (const connection of open) connection.socket.terminate();
      await closed;
    }
    // Presence departures are published from each connection's queue; wait for them.
    await Promise.all(open.map((connection) => connection.settled()));

    this.wss.close();
    await this.adapter.close();
    const stopped = new Promise<void>((resolve) => this.http.close(() => resolve()));
    this.http.closeAllConnections();
    await stopped;
    this.logger.info('stopped');
  }
}

function toFrame(event: RoomEvent): { frame: ServerMessage; delivery: Delivery; origin?: string } {
  switch (event.kind) {
    case 'message':
      return { frame: { v: 1, type: 'message', ...event.message }, delivery: 'essential' };
    case 'typing':
      return {
        frame: { v: 1, type: 'typing', room: event.room, user: event.user, isTyping: event.isTyping },
        delivery: 'droppable',
        origin: event.connectionId,
      };
    case 'presence':
      return {
        frame: { v: 1, type: 'presence', room: event.room, event: event.event, member: event.member },
        delivery: 'essential',
        origin: event.connectionId,
      };
  }
}

function rejectUpgrade(socket: Duplex, status: number, headers: Record<string, string> = {}): void {
  const body = STATUS_CODES[status] ?? 'Error';
  const lines = [
    `HTTP/1.1 ${status} ${body}`,
    'Connection: close',
    'Content-Type: text/plain',
    `Content-Length: ${Buffer.byteLength(body)}`,
    ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
  ];
  socket.end(`${lines.join('\r\n')}\r\n\r\n${body}`);
}
