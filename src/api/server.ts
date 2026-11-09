import http from 'node:http';
import { env } from '../config/env';
import { logger } from '../config/logger';
import { createRedis } from '../infra/redis/client';
import { RabbitConnection } from '../infra/rabbitmq/connection';
import { InventoryService } from '../modules/inventory/inventory.service';
import { OrderEventPublisher } from '../modules/messaging/orderEventPublisher';
import { PolicyProvider, RateLimiter } from '../modules/ratelimit/rateLimiter';
import { createApp } from './app';

async function main(): Promise<void> {
  const redis = createRedis();
  const rabbit = new RabbitConnection(env.RABBITMQ_URL);
  const publisher = new OrderEventPublisher(rabbit);
  await publisher.init(); // fail fast if the broker is unreachable at boot

  const inventory = new InventoryService(redis);
  const limiter = new RateLimiter(redis, new PolicyProvider(redis));

  const app = createApp({
    inventory,
    publisher,
    limiter,
    isReady: () => redis.status === 'ready' && publisher.isReady(),
  });

  const server = http.createServer(app);
  server.keepAliveTimeout = 65_000; // > typical LB idle timeout, avoids spurious 502s
  server.headersTimeout = 66_000;
  server.listen(env.PORT, () => logger.info({ port: env.PORT }, 'api listening'));

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    server.close(); // stop accepting; in-flight requests finish
    setTimeout(() => process.exit(1), 15_000).unref();
    await publisher.close();
    await rabbit.close();
    await redis.quit().catch(() => undefined);
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

process.on('unhandledRejection', (err) => logger.error({ err }, 'unhandled rejection'));
main().catch((err) => {
  logger.fatal({ err }, 'api failed to start');
  process.exit(1);
});
