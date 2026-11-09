import type { Pool } from 'pg';
import { withTransaction } from '../../infra/postgres';
import type { OrderCreatedData } from './order.events';

export interface ConfirmedOrderInput {
  data: OrderCreatedData;
  amountCents: number;
  paymentId: string;
}

/**
 * All writes are append-only and idempotent (ON CONFLICT DO NOTHING): redelivered messages,
 * worker crashes between commit and ack, and duplicate publishes are all harmless.
 * No UPDATE on a shared stock row anywhere => no hot-row lock contention in PostgreSQL.
 */
export class OrderRepository {
  private readonly priceCache = new Map<string, { cents: number; expiresAt: number }>();

  constructor(private readonly pool: Pool) {}

  async exists(orderId: string): Promise<boolean> {
    const { rowCount } = await this.pool.query('SELECT 1 FROM orders WHERE order_id = $1', [orderId]);
    return (rowCount ?? 0) > 0;
  }

  /** Price lookup with a 30s cache (read-only, lock-free). */
  async getPriceCents(productId: string): Promise<number | null> {
    const hit = this.priceCache.get(productId);
    if (hit && hit.expiresAt > Date.now()) return hit.cents;

    const { rows } = await this.pool.query<{ price_cents: number }>(
      'SELECT price_cents FROM products WHERE id = $1',
      [productId],
    );
    if (rows.length === 0) return null;
    const cents = rows[0].price_cents;
    this.priceCache.set(productId, { cents, expiresAt: Date.now() + 30_000 });
    return cents;
  }

  /** ACID: order + payment + audit event commit together or not at all. */
  async saveConfirmed({ data, amountCents, paymentId }: ConfirmedOrderInput): Promise<'created' | 'duplicate'> {
    return withTransaction(this.pool, async (client) => {
      const inserted = await client.query(
        `INSERT INTO orders (order_id, user_id, product_id, quantity, amount_cents, status, idempotency_key)
         VALUES ($1, $2, $3, $4, $5, 'CONFIRMED', $6)
         ON CONFLICT DO NOTHING
         RETURNING id`,
        [data.orderId, data.userId, data.productId, data.quantity, amountCents, data.idempotencyKey],
      );
      if (inserted.rowCount === 0) return 'duplicate';

      await client.query(
        `INSERT INTO payments (payment_id, order_id, amount_cents, status)
         VALUES ($1, $2, $3, 'SUCCEEDED')
         ON CONFLICT DO NOTHING`,
        [paymentId, data.orderId, amountCents],
      );
      await client.query(
        `INSERT INTO order_events (order_id, event_type, payload) VALUES ($1, 'OrderConfirmed', $2)`,
        [data.orderId, JSON.stringify(data)],
      );
      return 'created';
    });
  }

  async saveFailed(data: OrderCreatedData, reason: string): Promise<void> {
    await withTransaction(this.pool, async (client) => {
      const inserted = await client.query(
        `INSERT INTO orders (order_id, user_id, product_id, quantity, amount_cents, status, failure_reason, idempotency_key)
         VALUES ($1, $2, $3, $4, 0, 'FAILED', $5, $6)
         ON CONFLICT DO NOTHING
         RETURNING id`,
        [data.orderId, data.userId, data.productId, data.quantity, reason, data.idempotencyKey],
      );
      if ((inserted.rowCount ?? 0) > 0) {
        await client.query(
          `INSERT INTO order_events (order_id, event_type, payload) VALUES ($1, 'OrderFailed', $2)`,
          [data.orderId, JSON.stringify({ ...data, reason })],
        );
      }
    });
  }
}
