import type Redis from 'ioredis';
import { env } from '../../config/env';
import { metrics } from '../../infra/metrics';

export interface BucketPolicy {
  capacity: number; // burst size
  refillPerSec: number; // sustained rate
}

export type LimitScope = 'global' | 'user';

export interface RateLimitDecision {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
}

/**
 * Dynamic policies: operators can retune limits LIVE, with no deploy, e.g.
 *   redis-cli HSET ratelimit:policy:global capacity 8000 refillPerSec 6000
 *   redis-cli HSET ratelimit:policy:user   capacity 3    refillPerSec 0.5
 * Each API instance caches a policy for `ttlMs` to keep the hot path to a single Redis call.
 */
export class PolicyProvider {
  private readonly cache = new Map<string, { policy: BucketPolicy; expiresAt: number }>();

  private readonly defaults: Record<LimitScope, BucketPolicy> = {
    global: { capacity: env.RL_GLOBAL_CAPACITY, refillPerSec: env.RL_GLOBAL_REFILL_PER_SEC },
    user: { capacity: env.RL_USER_CAPACITY, refillPerSec: env.RL_USER_REFILL_PER_SEC },
  };

  constructor(
    private readonly redis: Redis,
    private readonly ttlMs = 5_000,
  ) {}

  async get(scope: LimitScope): Promise<BucketPolicy> {
    const now = Date.now();
    const hit = this.cache.get(scope);
    if (hit && hit.expiresAt > now) return hit.policy;

    let policy = this.defaults[scope];
    try {
      const raw = await this.redis.hgetall(`ratelimit:policy:${scope}`);
      const capacity = Number(raw.capacity);
      const refillPerSec = Number(raw.refillPerSec);
      if (capacity > 0 && refillPerSec > 0) policy = { capacity, refillPerSec };
    } catch {
      // keep defaults if Redis hiccups; the limiter itself fails open
    }
    this.cache.set(scope, { policy, expiresAt: now + this.ttlMs });
    return policy;
  }
}

export class RateLimiter {
  constructor(
    private readonly redis: Redis,
    private readonly policies: PolicyProvider,
  ) {}

  async consume(scope: LimitScope, identity = '-', cost = 1): Promise<RateLimitDecision> {
    const policy = await this.policies.get(scope);
    const key = scope === 'global' ? 'rl:global' : `rl:user:${identity}`;

    const [allowed, remaining, retryAfterMs] = await this.redis.tokenBucket(
      key,
      policy.capacity,
      policy.refillPerSec,
      cost,
    );
    metrics.rateLimitDecisions.inc({ scope, result: allowed === 1 ? 'allowed' : 'rejected' });
    return { allowed: allowed === 1, remaining, retryAfterMs };
  }
}
