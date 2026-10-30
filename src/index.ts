#!/usr/bin/env node
import type { ClusterAdapter } from './cluster/adapter.js';
import { InMemoryAdapter, InMemoryBroker } from './cluster/memory.js';
import { RedisAdapter } from './cluster/redis.js';
import { loadConfig } from './config.js';
import { createLogger } from './logger.js';
import { ChatServer } from './server.js';

async function main(): Promise<void> {
  const config = loadConfig(process.env);
  const logger = createLogger(config.LOG_LEVEL, { service: 'realtime-presence-chat' });

  const adapter: ClusterAdapter = config.REDIS_URL
    ? await RedisAdapter.connect(config.REDIS_URL, {
        nodeId: config.NODE_ID,
        keyPrefix: config.REDIS_KEY_PREFIX,
        historySize: config.HISTORY_SIZE,
      })
    : new InMemoryAdapter(new InMemoryBroker(config.HISTORY_SIZE), config.NODE_ID);
  logger.info('cluster adapter ready', { adapter: config.REDIS_URL ? 'redis' : 'memory', nodeId: adapter.nodeId });

  if (config.DEV_TOKENS) logger.warn('DEV_TOKENS is enabled: anyone can mint tokens at /dev/token');

  const server = new ChatServer({
    adapter,
    logger,
    port: config.PORT,
    host: config.HOST,
    jwtSecret: config.JWT_SECRET,
    jwtIssuer: config.JWT_ISSUER,
    jwtAudience: config.JWT_AUDIENCE,
    heartbeatIntervalMs: config.HEARTBEAT_INTERVAL_MS,
    rateLimit: {
      capacity: config.RATE_LIMIT_CAPACITY,
      refillPerSecond: config.RATE_LIMIT_REFILL_PER_SECOND,
      maxViolations: config.RATE_LIMIT_MAX_VIOLATIONS,
    },
    backpressure: { softLimitBytes: config.BACKPRESSURE_SOFT_BYTES, hardLimitBytes: config.BACKPRESSURE_HARD_BYTES },
    maxPayloadBytes: config.MAX_PAYLOAD_BYTES,
    maxRoomsPerConnection: config.MAX_ROOMS_PER_CONNECTION,
    shutdownGraceMs: config.SHUTDOWN_GRACE_MS,
    devTokens: config.DEV_TOKENS,
  });
  await server.listen();

  let stopping = false;
  const stop = (signal: NodeJS.Signals) => {
    if (stopping) return;
    stopping = true;
    logger.info('signal received', { signal });
    server.close().then(
      () => process.exit(0),
      (error: unknown) => {
        logger.error('shutdown failed', { error });
        process.exit(1);
      },
    );
  };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
