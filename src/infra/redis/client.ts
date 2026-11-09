import fs from 'node:fs';
import path from 'node:path';
import Redis from 'ioredis';
import { env } from '../../config/env';
import { logger } from '../../config/logger';

/**
 * Custom commands registered via ioredis `defineCommand`.
 * ioredis transparently uses EVALSHA and falls back to EVAL after a NOSCRIPT error.
 *
 * Return tuples:
 *   reserveStock -> [code, orderId, remaining]   code: 1 reserved | 0 sold out | -1 duplicate | -2 not warmed | -3 user limit
 *   releaseStock -> 1 released | 0 already released (no-op)
 *   tokenBucket  -> [allowed(0|1), tokensRemaining, retryAfterMs]
 */
declare module 'ioredis' {
  interface RedisCommander<Context> {
    reserveStock(
      stockKey: string,
      idempotencyKey: string,
      userPurchasedKey: string,
      quantity: number,
      orderId: string,
      idempotencyTtlSec: number,
      perUserLimit: number,
    ): Promise<[number, string, number]>;

    releaseStock(
      stockKey: string,
      releasedFlagKey: string,
      userPurchasedKey: string,
      idempotencyKey: string,
      quantity: number,
      flagTtlSec: number,
      deleteIdempotencyKey: number,
    ): Promise<number>;

    tokenBucket(
      bucketKey: string,
      capacity: number,
      refillPerSec: number,
      requested: number,
    ): Promise<[number, number, number]>;
  }
}

const SCRIPT_DIR = path.join(__dirname, 'scripts');
const loadScript = (file: string): string => fs.readFileSync(path.join(SCRIPT_DIR, file), 'utf8');

export function createRedis(): Redis {
  const redis = new Redis(env.REDIS_URL, {
    enableAutoPipelining: true, // coalesces concurrent commands into pipelines: big win at 2k+ rps
    maxRetriesPerRequest: 2,
    connectTimeout: 5_000,
    commandTimeout: 1_000, // fail fast on the hot path instead of piling up requests
  });
  redis.on('error', (err) => logger.error({ err }, 'redis error'));

  redis.defineCommand('reserveStock', { numberOfKeys: 3, lua: loadScript('reserve_stock.lua') });
  redis.defineCommand('releaseStock', { numberOfKeys: 4, lua: loadScript('release_stock.lua') });
  redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: loadScript('token_bucket.lua') });
  return redis;
}

export type { Redis };
