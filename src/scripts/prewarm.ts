import { logger } from '../config/logger';
import { createPool } from '../infra/postgres';
import { createRedis } from '../infra/redis/client';
import { InventoryService } from '../modules/inventory/inventory.service';

/**
 * Pre-warm stock into Redis before a sale starts.
 *   node dist/scripts/prewarm.js            # every product whose sale has not ended
 *   node dist/scripts/prewarm.js sku-001    # one product
 *
 * available = total_stock - units already durably sold, written with SET NX so that running this
 * again mid-sale cannot reset live counters.
 */
async function main(): Promise<void> {
  const pool = createPool();
  const redis = createRedis();
  const inventory = new InventoryService(redis);
  const only = process.argv[2] ?? null;

  try {
    const { rows } = await pool.query<{ id: string; total_stock: number; units_sold: number }>(
      `SELECT p.id, p.total_stock, COALESCE(s.units_sold, 0) AS units_sold
         FROM products p
         LEFT JOIN product_sales s ON s.product_id = p.id
        WHERE p.sale_ends_at > now()
          AND ($1::text IS NULL OR p.id = $1)`,
      [only],
    );

    for (const row of rows) {
      const available = Math.max(0, row.total_stock - row.units_sold);
      const created = await inventory.prewarm(row.id, available);
      logger.info(
        { productId: row.id, available, created },
        created ? 'stock pre-warmed' : 'stock key already present, left untouched',
      );
    }
    if (rows.length === 0) logger.warn('no matching products to pre-warm');
  } finally {
    await pool.end();
    await redis.quit();
  }
}

main().catch((err) => {
  logger.fatal({ err }, 'prewarm failed');
  process.exit(1);
});
