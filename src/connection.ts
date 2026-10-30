import { WebSocket } from 'ws';
import type { ChatMetrics, DisconnectReason } from './metrics.js';
import type { Member, ServerMessage } from './protocol.js';
import { TokenBucket } from './rateLimiter.js';

export interface BackpressurePolicy {
  /** Above this many buffered bytes, droppable frames (typing indicators) are skipped. */
  softLimitBytes: number;
  /** Above this, the client is too slow to keep up and is disconnected. */
  hardLimitBytes: number;
}

export interface ConnectionOptions {
  backpressure: BackpressurePolicy;
  rateLimit: { capacity: number; refillPerSecond: number };
  metrics: ChatMetrics;
  onSlowConsumer: (connection: ClientConnection) => void;
}

export type Delivery = 'essential' | 'droppable';
export type SendOutcome = 'sent' | 'dropped' | 'disconnected';

interface Pending {
  frame: ServerMessage;
  delivery: Delivery;
}

/** One authenticated socket: its user, rooms, rate limiter, heartbeat flag and send policy. */
export class ClientConnection {
  readonly rooms = new Set<string>();
  /** Rooms whose join snapshot is still being assembled; live events wait here meanwhile. */
  readonly syncing = new Map<string, Pending[]>();
  readonly bucket: TokenBucket;
  isAlive = true;
  /** Consecutive frames rejected by the rate limiter. */
  violations = 0;
  closedBySlowConsumer = false;
  /** Why the server is closing this connection; undefined means the client closed it. */
  closeReason: DisconnectReason | undefined;
  private queue: Promise<void> = Promise.resolve();

  constructor(
    readonly id: string,
    readonly user: Member,
    readonly socket: WebSocket,
    private readonly options: ConnectionOptions,
  ) {
    this.bucket = new TokenBucket(options.rateLimit.capacity, options.rateLimit.refillPerSecond);
  }

  /**
   * Sends a frame unless the socket has fallen behind. `bufferedAmount` counts bytes accepted by
   * `send` but not yet written to the kernel, so it grows when the peer reads slower than we write.
   */
  send(frame: ServerMessage, delivery: Delivery = 'essential'): SendOutcome {
    if (this.socket.readyState !== WebSocket.OPEN) return 'dropped';
    const buffered = this.socket.bufferedAmount;
    if (buffered > this.options.backpressure.hardLimitBytes) {
      this.options.metrics.framesDropped.inc({ reason: 'slow_consumer' });
      if (!this.closedBySlowConsumer) {
        this.closedBySlowConsumer = true;
        this.options.onSlowConsumer(this);
      }
      return 'disconnected';
    }
    if (delivery === 'droppable' && buffered > this.options.backpressure.softLimitBytes) {
      this.options.metrics.framesDropped.inc({ reason: 'backpressure' });
      return 'dropped';
    }
    this.socket.send(JSON.stringify(frame));
    this.options.metrics.framesSent.inc({ type: frame.type });
    return 'sent';
  }

  /** Delivers a room event, or holds it back while the room's join snapshot is in flight. */
  deliver(room: string, frame: ServerMessage, delivery: Delivery): void {
    const pending = this.syncing.get(room);
    if (pending) {
      pending.push({ frame, delivery });
      return;
    }
    this.send(frame, delivery);
  }

  /**
   * Runs work for this connection strictly in order (joins, messages, the final clean-up), so a
   * disconnect can never overtake a join that is still talking to the adapter.
   */
  enqueue(work: () => Promise<void>): Promise<void> {
    const next = this.queue.then(work);
    this.queue = next.catch(() => undefined);
    return next;
  }

  /** Resolves once all queued work has finished. */
  settled(): Promise<void> {
    return this.queue;
  }
}
