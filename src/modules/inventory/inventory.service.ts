import { randomUUID } from 'node:crypto';
import type Redis from 'ioredis';
import { env } from '../../config/env';
import { metrics } from '../../infra/metrics';

/**
 * Redis key layout. Everything for one product shares the `{productId}` hash tag so the
 * multi-key Lua scripts stay valid on Redis Cluster (all keys hash to the same slot).
 */
export const redisKeys = {
  stock: (productId: string) => `stock:{${productId}}`,
  idempotency: (productId: string, userId: string, key: string) => `idem:{${productId}}:${userId}:${key}`,
  purchased: (productId: string, userId: string) => `purchased:{${productId}}:${userId}`,
  released: (productId: string, orderId: string) => `released:{${productId}}:${orderId}`,
};

export interface ReserveInput {
  productId: string;
  userId: string;
  idempotencyKey: string;
  quantity: number;
}

export type ReserveOutcome =
  | { status: 'RESERVED'; orderId: string; remaining: number }
  | { status: 'DUPLICATE'; orderId: string }
  | { status: 'SOLD_OUT' }
  | { status: 'USER_LIMIT' }
  | { status: 'NOT_WARMED' };

export interface ReleaseInput {
  productId: string;
  userId: string;
  orderId: string;
  idempotencyKey: string;
  quantity: number;
  /** true when the client should be able to retry with the same Idempotency-Key. */
  deleteIdempotencyKey?: boolean;
}

const RELEASED_FLAG_TTL_SEC = 7 * 24 * 3600;

export class InventoryService {
  constructor(private readonly redis: Redis) {}

  /**
   * Pre-warm: load sellable units into Redis before the sale opens.
   * Uses SET NX so re-running the job can never overwrite live (already decremented) stock.
   * @returns true if the key was created, false if it already existed.
   */
  async prewarm(productId: string, units: number): Promise<boolean> {
    const res = await this.redis.set(redisKeys.stock(productId), units, 'NX');
    return res === 'OK';
  }

  async available(productId: string): Promise<number | null> {
    const v = await this.redis.get(redisKeys.stock(productId));
    return v === null ? null : Number(v);
  }

  /** Atomic reservation (single Lua call). Throws if Redis is unreachable: we fail closed, never oversell. */
  async reserve(input: ReserveInput): Promise<ReserveOutcome> {
    const { productId, userId, idempotencyKey, quantity } = input;
    const candidateOrderId = randomUUID();

    const [code, orderId, remaining] = await this.redis.reserveStock(
      redisKeys.stock(productId),
      redisKeys.idempotency(productId, userId, idempotencyKey),
      redisKeys.purchased(productId, userId),
      quantity,
      candidateOrderId,
      env.IDEMPOTENCY_TTL_SEC,
      env.PER_USER_PURCHASE_LIMIT,
    );

    switch (code) {
      case 1:
        metrics.stockReservations.inc({ result: 'reserved' });
        return { status: 'RESERVED', orderId, remaining };
      case -1:
        metrics.stockReservations.inc({ result: 'duplicate' });
        return { status: 'DUPLICATE', orderId };
      case 0:
        metrics.stockReservations.inc({ result: 'sold_out' });
        return { status: 'SOLD_OUT' };
      case -3:
        metrics.stockReservations.inc({ result: 'user_limit' });
        return { status: 'USER_LIMIT' };
      default:
        metrics.stockReservations.inc({ result: 'not_warmed' });
        return { status: 'NOT_WARMED' };
    }
  }

  /** Idempotent compensation. Returns true if units were actually returned to stock. */
  async release(input: ReleaseInput): Promise<boolean> {
    const { productId, userId, orderId, idempotencyKey, quantity, deleteIdempotencyKey = false } = input;
    const res = await this.redis.releaseStock(
      redisKeys.stock(productId),
      redisKeys.released(productId, orderId),
      redisKeys.purchased(productId, userId),
      redisKeys.idempotency(productId, userId, idempotencyKey),
      quantity,
      RELEASED_FLAG_TTL_SEC,
      deleteIdempotencyKey ? 1 : 0,
    );
    return res === 1;
  }

  /** True if this order's reservation was already cancelled (API publish failure or worker failure). */
  async wasReleased(productId: string, orderId: string): Promise<boolean> {
    return (await this.redis.exists(redisKeys.released(productId, orderId))) === 1;
  }
}
