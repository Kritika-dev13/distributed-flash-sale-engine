import { Router, type RequestHandler } from 'express';
import { logger } from '../../config/logger';
import { metrics } from '../../infra/metrics';
import type { InventoryService, ReserveOutcome } from '../inventory/inventory.service';
import type { OrderEventPublisher } from '../messaging/orderEventPublisher';
import { rateLimitMiddleware } from '../ratelimit/rateLimit.middleware';
import type { RateLimiter } from '../ratelimit/rateLimiter';
import { buildOrderCreated } from './order.events';
import { checkoutBodySchema, checkoutHeadersSchema } from './orders.schema';

export interface OrdersRouterDeps {
  inventory: InventoryService;
  publisher: OrderEventPublisher;
  limiter: RateLimiter;
}

export function createOrdersRouter(deps: OrdersRouterDeps): Router {
  const router = Router();
  // POST /api/v1/orders/checkout
  router.post('/checkout', rateLimitMiddleware(deps.limiter), checkoutHandler(deps));
  return router;
}

/**
 * Hot path (no PostgreSQL access at all):
 *   validate -> [Lua] dedupe + stock check + decrement -> publish OrderCreated (confirmed) -> 202
 */
function checkoutHandler({ inventory, publisher }: OrdersRouterDeps): RequestHandler {
  return async (req, res) => {
    const stopTimer = metrics.checkoutIngestionDuration.startTimer();
    let outcome = 'error';

    try {
      // ---- 1. Validate -------------------------------------------------------------------
      const headers = checkoutHeadersSchema.safeParse(req.headers);
      const body = checkoutBodySchema.safeParse(req.body ?? {});
      if (!headers.success || !body.success) {
        outcome = 'invalid';
        res.status(400).json({
          code: 'VALIDATION_FAILED',
          issues: [
            ...(headers.success ? [] : headers.error.issues),
            ...(body.success ? [] : body.error.issues),
          ].map((i) => ({ path: i.path.join('.'), message: i.message })),
        });
        return;
      }
      const idempotencyKey = headers.data['idempotency-key'];
      const userId = headers.data['x-user-id'];
      const { productId, quantity } = body.data;

      // ---- 2. Atomic reservation in Redis ---------------------------------------------------
      let result: ReserveOutcome;
      try {
        result = await inventory.reserve({ productId, userId, idempotencyKey, quantity });
      } catch (err) {
        outcome = 'redis_error';
        logger.error({ err }, 'stock reservation failed');
        res.status(503).set('Retry-After', '1').json({ code: 'INVENTORY_UNAVAILABLE' });
        return;
      }

      switch (result.status) {
        case 'DUPLICATE':
          // Same Idempotency-Key => same answer, nothing new is reserved or published.
          outcome = 'duplicate';
          res
            .status(202)
            .set('Idempotent-Replayed', 'true')
            .json({ orderId: result.orderId, status: 'ACCEPTED', replayed: true });
          return;
        case 'SOLD_OUT':
          outcome = 'sold_out';
          res.status(409).json({ code: 'SOLD_OUT', message: 'This item is sold out' });
          return;
        case 'USER_LIMIT':
          outcome = 'user_limit';
          res.status(409).json({ code: 'PURCHASE_LIMIT_EXCEEDED' });
          return;
        case 'NOT_WARMED':
          outcome = 'not_warmed';
          res.status(404).json({ code: 'SALE_NOT_FOUND' });
          return;
        case 'RESERVED':
          break;
      }

      // ---- 3. Publish OrderCreated (waits for the broker's publisher confirm) -------------
      const event = buildOrderCreated({
        orderId: result.orderId,
        userId,
        productId,
        quantity,
        idempotencyKey,
      });

      try {
        await publisher.publishOrderCreated(event);
      } catch (err) {
        // Compensate: return the units and free the idempotency key so the client can retry safely.
        outcome = 'publish_failed';
        logger.error({ err, orderId: result.orderId }, 'publish failed, releasing reservation');
        await inventory
          .release({
            productId,
            userId,
            orderId: result.orderId,
            idempotencyKey,
            quantity,
            deleteIdempotencyKey: true,
          })
          .catch((releaseErr) => logger.error({ releaseErr, orderId: result.orderId }, 'release failed'));
        res.status(503).set('Retry-After', '1').json({ code: 'ORDER_QUEUE_UNAVAILABLE' });
        return;
      }

      // ---- 4. Accepted -----------------------------------------------------------------------
      outcome = 'accepted';
      res.status(202).json({ orderId: result.orderId, status: 'ACCEPTED' });
    } catch (err) {
      logger.error({ err }, 'checkout handler crashed');
      if (!res.headersSent) res.status(500).json({ code: 'INTERNAL_ERROR' });
    } finally {
      stopTimer({ outcome });
    }
  };
}
