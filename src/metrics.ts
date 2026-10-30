import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';

export type DisconnectReason =
  | 'client'
  | 'heartbeat_timeout'
  | 'slow_consumer'
  | 'rate_limit'
  | 'token_expired'
  | 'shutdown';

/**
 * Prometheus metrics. Each server owns its registry (not the prom-client global), so several
 * servers can run in one process without clashing.
 */
export class ChatMetrics {
  readonly registry = new Registry();

  readonly connections = new Gauge({
    name: 'chat_connections',
    help: 'Open WebSocket connections',
    registers: [this.registry],
  });
  readonly connectionsOpened = new Counter({
    name: 'chat_connections_opened_total',
    help: 'Accepted WebSocket connections',
    registers: [this.registry],
  });
  readonly disconnects = new Counter({
    name: 'chat_disconnects_total',
    help: 'Closed connections by reason',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  readonly authFailures = new Counter({
    name: 'chat_auth_failures_total',
    help: 'Rejected upgrade requests by reason',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  readonly framesReceived = new Counter({
    name: 'chat_frames_received_total',
    help: 'Inbound frames by message type (invalid frames as "invalid")',
    labelNames: ['type'] as const,
    registers: [this.registry],
  });
  readonly framesSent = new Counter({
    name: 'chat_frames_sent_total',
    help: 'Outbound frames by message type',
    labelNames: ['type'] as const,
    registers: [this.registry],
  });
  readonly framesDropped = new Counter({
    name: 'chat_frames_dropped_total',
    help: 'Frames not processed or not delivered, by reason',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  readonly localRooms = new Gauge({
    name: 'chat_local_rooms',
    help: 'Rooms with at least one member connected to this node',
    registers: [this.registry],
  });
  readonly handleSeconds = new Histogram({
    name: 'chat_inbound_handle_seconds',
    help: 'Time to handle an inbound frame, including adapter round trips',
    labelNames: ['type'] as const,
    buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1],
    registers: [this.registry],
  });

  constructor(options: { collectProcessMetrics?: boolean } = {}) {
    if (options.collectProcessMetrics ?? true) collectDefaultMetrics({ register: this.registry });
  }
}
