import { z } from 'zod';

const flag = z
  .enum(['true', 'false', '1', '0'])
  .default('false')
  .transform((value) => value === 'true' || value === '1');
const int = (fallback: number, min = 1) => z.coerce.number().int().min(min).default(fallback);

const EnvSchema = z
  .object({
    PORT: int(8080, 0),
    HOST: z.string().default('0.0.0.0'),
    JWT_SECRET: z.string().min(32, 'JWT_SECRET must be at least 32 characters'),
    JWT_ISSUER: z.string().optional(),
    JWT_AUDIENCE: z.string().optional(),
    REDIS_URL: z.string().url().optional(),
    REDIS_KEY_PREFIX: z.string().default('chat:'),
    NODE_ID: z.string().optional(),
    HEARTBEAT_INTERVAL_MS: int(30_000, 100),
    RATE_LIMIT_CAPACITY: int(10),
    RATE_LIMIT_REFILL_PER_SECOND: z.coerce.number().positive().default(5),
    RATE_LIMIT_MAX_VIOLATIONS: int(20),
    BACKPRESSURE_SOFT_BYTES: int(256 * 1024),
    BACKPRESSURE_HARD_BYTES: int(1024 * 1024),
    MAX_PAYLOAD_BYTES: int(16 * 1024, 256),
    MAX_ROOMS_PER_CONNECTION: int(20),
    HISTORY_SIZE: int(50),
    SHUTDOWN_GRACE_MS: int(5_000, 0),
    DEV_TOKENS: flag,
    LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error', 'silent']).default('info'),
  })
  .refine((env) => env.BACKPRESSURE_SOFT_BYTES <= env.BACKPRESSURE_HARD_BYTES, {
    message: 'BACKPRESSURE_SOFT_BYTES must not exceed BACKPRESSURE_HARD_BYTES',
  });

export type Config = z.infer<typeof EnvSchema>;

export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const parsed = EnvSchema.safeParse(env);
  if (!parsed.success) throw new Error(`invalid configuration:\n${z.prettifyError(parsed.error)}`);
  return parsed.data;
}
