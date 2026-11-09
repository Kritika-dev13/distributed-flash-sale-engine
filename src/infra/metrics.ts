import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

// Fine-grained buckets: p50/p95/p99 are derived with histogram_quantile() in PromQL.
const LATENCY_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

export const metrics = {
  // ---- API ----
  httpRequestDuration: new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency',
    labelNames: ['method', 'route', 'status_code'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [registry],
  }),
  httpRequestsTotal: new client.Counter({
    name: 'http_requests_total',
    help: 'HTTP requests by status code',
    labelNames: ['method', 'route', 'status_code'] as const,
    registers: [registry],
  }),
  checkoutIngestionDuration: new client.Histogram({
    name: 'checkout_ingestion_duration_seconds',
    help: 'Time inside the checkout handler (Redis reservation + broker publish confirm)',
    labelNames: ['outcome'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [registry],
  }),
  // "Lock" hits/misses: a hit = stock atomically claimed; a miss = sold out / limit / duplicate.
  stockReservations: new client.Counter({
    name: 'stock_reservation_total',
    help: 'Result of the atomic Redis stock reservation',
    labelNames: ['result'] as const, // reserved | sold_out | duplicate | user_limit | not_warmed
    registers: [registry],
  }),
  rateLimitDecisions: new client.Counter({
    name: 'rate_limit_decisions_total',
    help: 'Token-bucket decisions',
    labelNames: ['scope', 'result'] as const, // scope: global|user ; result: allowed|rejected|fail_open
    registers: [registry],
  }),
  rabbitmqPublish: new client.Counter({
    name: 'rabbitmq_publish_total',
    help: 'Publisher-confirm outcomes for OrderCreated events',
    labelNames: ['result'] as const,
    registers: [registry],
  }),

  // ---- Worker ----
  queueDepth: new client.Gauge({
    name: 'rabbitmq_queue_depth',
    help: 'Messages ready in a queue (polled by workers)',
    labelNames: ['queue'] as const,
    registers: [registry],
  }),
  ordersProcessed: new client.Counter({
    name: 'orders_processed_total',
    help: 'Worker outcomes',
    labelNames: ['outcome'] as const, // confirmed|duplicate|cancelled|declined|retried|deferred|dead_lettered|malformed
    registers: [registry],
  }),
  orderProcessingDuration: new client.Histogram({
    name: 'order_processing_duration_seconds',
    help: 'Worker processing time per message',
    buckets: LATENCY_BUCKETS,
    registers: [registry],
  }),
  circuitBreakerState: new client.Gauge({
    name: 'circuit_breaker_state',
    help: '0 = closed, 1 = half-open, 2 = open',
    labelNames: ['name'] as const,
    registers: [registry],
  }),
  circuitBreakerEvents: new client.Counter({
    name: 'circuit_breaker_events_total',
    help: 'Opossum breaker events',
    labelNames: ['name', 'event'] as const,
    registers: [registry],
  }),
};
