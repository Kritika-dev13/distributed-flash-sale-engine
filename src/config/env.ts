import { z } from 'zod';

/**
 * Single, validated source of truth for configuration.
 * The process fails fast at boot if anything is malformed.
 */
const schema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  PORT: z.coerce.number().int().positive().default(3000),
  WORKER_METRICS_PORT: z.coerce.number().int().positive().default(9101),

  REDIS_URL: z.string().default('redis://localhost:6379'),
  RABBITMQ_URL: z.string().default('amqp://flash:flash@localhost:5672'),
  DATABASE_URL: z.string().default('postgres://flash:flash@localhost:5432/flashsale'),
  PG_POOL_MAX: z.coerce.number().int().positive().default(20),

  // Business rules
  IDEMPOTENCY_TTL_SEC: z.coerce.number().int().positive().default(86_400),
  PER_USER_PURCHASE_LIMIT: z.coerce.number().int().nonnegative().default(2), // 0 = unlimited
  MAX_QUANTITY_PER_ORDER: z.coerce.number().int().positive().default(2),
  PUBLISH_TIMEOUT_MS: z.coerce.number().int().positive().default(2_000),

  // Token bucket defaults (can be overridden at runtime through Redis, see ratelimit/rateLimiter.ts)
  RL_GLOBAL_CAPACITY: z.coerce.number().positive().default(6_000),
  RL_GLOBAL_REFILL_PER_SEC: z.coerce.number().positive().default(4_000),
  RL_USER_CAPACITY: z.coerce.number().positive().default(5),
  RL_USER_REFILL_PER_SEC: z.coerce.number().positive().default(1),

  // Worker
  WORKER_PREFETCH: z.coerce.number().int().positive().default(50),
  MAX_BREAKER_DEFERRALS: z.coerce.number().int().nonnegative().default(20),

  // Payment gateway simulation + circuit breaker
  PAYMENT_TIMEOUT_MS: z.coerce.number().int().positive().default(3_000),
  PAYMENT_LATENCY_MS: z.coerce.number().int().nonnegative().default(40),
  PAYMENT_TRANSIENT_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0.05),
  PAYMENT_DECLINE_RATE: z.coerce.number().min(0).max(1).default(0.02),
  BREAKER_ERROR_THRESHOLD_PCT: z.coerce.number().min(1).max(100).default(50),
  BREAKER_RESET_MS: z.coerce.number().int().positive().default(10_000),
  BREAKER_VOLUME_THRESHOLD: z.coerce.number().int().positive().default(20),
});

export const env = schema.parse(process.env);
export type Env = z.infer<typeof schema>;
