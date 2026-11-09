import type { RequestHandler, Response } from 'express';
import { logger } from '../../config/logger';
import { metrics } from '../../infra/metrics';
import type { LimitScope, RateLimiter } from './rateLimiter';

function reject(res: Response, scope: LimitScope, retryAfterMs: number): void {
  res
    .status(429)
    .set('Retry-After', String(Math.max(1, Math.ceil(retryAfterMs / 1000))))
    .set('X-RateLimit-Scope', scope)
    .json({ code: 'RATE_LIMITED', scope, retryAfterMs });
}

/**
 * Two-tier token bucket: a global bucket protects the system as a whole,
 * a per-user bucket stops one client (or bot) from monopolising the global budget.
 * FAILS OPEN on limiter errors: the stock Lua script is the real oversell guard,
 * so availability of the endpoint is preferred over strictness of the limiter.
 */
export function rateLimitMiddleware(limiter: RateLimiter): RequestHandler {
  return async (req, res, next) => {
    try {
      const global = await limiter.consume('global');
      if (!global.allowed) {
        reject(res, 'global', global.retryAfterMs);
        return;
      }
      const identity = req.header('x-user-id') ?? req.ip ?? 'anonymous';
      const user = await limiter.consume('user', identity);
      if (!user.allowed) {
        reject(res, 'user', user.retryAfterMs);
        return;
      }
      next();
    } catch (err) {
      logger.warn({ err }, 'rate limiter unavailable, failing open');
      metrics.rateLimitDecisions.inc({ scope: 'global', result: 'fail_open' });
      next();
    }
  };
}
