import { Pool, type PoolClient } from 'pg';
import { env } from '../config/env';
import { logger } from '../config/logger';

export function createPool(): Pool {
  const pool = new Pool({
    connectionString: env.DATABASE_URL,
    max: env.PG_POOL_MAX,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
  pool.on('error', (err) => logger.error({ err }, 'idle postgres client error'));
  return pool;
}

/** Runs `fn` inside BEGIN/COMMIT, rolling back on any error. */
export async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
