import express, { type Express } from 'express';
import { registry } from '../infra/metrics';
import type { InventoryService } from '../modules/inventory/inventory.service';
import type { OrderEventPublisher } from '../modules/messaging/orderEventPublisher';
import { createOrdersRouter } from '../modules/orders/orders.router';
import type { RateLimiter } from '../modules/ratelimit/rateLimiter';
import { errorHandler, notFound } from './middleware/errorHandler';
import { httpMetrics } from './middleware/httpMetrics';

export interface AppDeps {
  inventory: InventoryService;
  publisher: OrderEventPublisher;
  limiter: RateLimiter;
  isReady: () => boolean;
}

export function createApp(deps: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', true);

  // Operational endpoints first: they bypass body parsing and request metrics.
  app.get('/healthz', (_req, res) => void res.status(200).send('ok'));
  app.get('/readyz', (_req, res) => void res.status(deps.isReady() ? 200 : 503).send(deps.isReady() ? 'ready' : 'not ready'));
  app.get('/metrics', async (_req, res) => {
    res.setHeader('Content-Type', registry.contentType);
    res.end(await registry.metrics());
  });

  app.use(httpMetrics);
  app.use(express.json({ limit: '4kb' }));
  app.use('/api/v1/orders', createOrdersRouter(deps));

  app.use(notFound);
  app.use(errorHandler);
  return app;
}
