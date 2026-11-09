import http from 'node:http';
import { env } from '../config/env';
import { logger } from '../config/logger';
import { registry } from '../infra/metrics';
import { createPool } from '../infra/postgres';
import { RabbitConnection } from '../infra/rabbitmq/connection';
import { createRedis } from '../infra/redis/client';
import { InventoryService } from '../modules/inventory/inventory.service';
import { OrderProcessor } from '../modules/orders/orderProcessor';
import { OrderRepository } from '../modules/orders/orders.repository';
import { PaymentBreaker } from '../modules/payment/paymentBreaker';
import { SimulatedPaymentGateway } from '../modules/payment/paymentGateway';
import { OrderConsumer } from './orderConsumer';
import { startQueueDepthPoller } from './queueDepthPoller';

async function main(): Promise<void> {
  const pool = createPool();
  const redis = createRedis();

  // If the broker connection drops, exit and let the orchestrator restart us:
  // simpler and safer than re-wiring consumers in place. Unacked messages are redelivered.
  const rabbit = new RabbitConnection(env.RABBITMQ_URL, () => {
    logger.error('rabbitmq connection lost, exiting for restart');
    process.exit(1);
  });

  const breaker = new PaymentBreaker(new SimulatedPaymentGateway());
  const processor = new OrderProcessor(new OrderRepository(pool), new InventoryService(redis), breaker);
  const consumer = new OrderConsumer(rabbit, processor);

  await consumer.start();
  const stopPoller = startQueueDepthPoller(await rabbit.createChannel());

  // Metrics + health endpoint for Prometheus
  const metricsServer = http
    .createServer(async (req, res) => {
      if (req.url === '/metrics') {
        res.setHeader('Content-Type', registry.contentType);
        res.end(await registry.metrics());
      } else if (req.url === '/healthz') {
        res.end('ok');
      } else {
        res.statusCode = 404;
        res.end();
      }
    })
    .listen(env.WORKER_METRICS_PORT, () => logger.info({ port: env.WORKER_METRICS_PORT }, 'worker metrics listening'));

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'worker shutting down');
    setTimeout(() => process.exit(1), 25_000).unref();
    stopPoller();
    await consumer.stop();
    breaker.shutdown();
    metricsServer.close();
    await rabbit.close();
    await pool.end().catch(() => undefined);
    await redis.quit().catch(() => undefined);
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

process.on('unhandledRejection', (err) => logger.error({ err }, 'unhandled rejection'));
main().catch((err) => {
  logger.fatal({ err }, 'worker failed to start');
  process.exit(1);
});
