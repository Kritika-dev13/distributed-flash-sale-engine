# Distributed Flash-Sale Processing Engine

Node.js (TypeScript) · Express · Redis (Lua) · RabbitMQ · PostgreSQL · Opossum · Prometheus · Grafana · Locust

## Quick start

```bash
docker compose up --build -d          # postgres, redis, rabbitmq, prewarm job, api, 2 workers, prometheus, grafana
curl -i -X POST localhost:3000/api/v1/orders/checkout \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: alice' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"productId":"sku-flash-001","quantity":1}'
# -> 202 {"orderId":"…","status":"ACCEPTED"}

docker compose --profile loadtest up locust        # Locust UI on :8089 (host http://api:3000 preset)
```

| UI | URL | Login |
|---|---|---|
| API | http://localhost:3000 | – |
| RabbitMQ management | http://localhost:15672 | flash / flash |
| Prometheus | http://localhost:9090 | – |
| Grafana | http://localhost:3001 | admin / admin |

## Request flow

```
client ──POST /checkout──▶ API (Express)
                            ├─ token bucket (Redis Lua): global + per-user      → 429
                            ├─ validate headers/body (zod)                      → 400
                            ├─ reserve_stock.lua (Redis, ATOMIC)                → 409 sold out / limit, 202 replay
                            │     dedupe idem-key · per-user cap · stock check · DECRBY · record order id
                            ├─ publish OrderCreated (RabbitMQ, publisher confirm) → on failure: release_stock.lua + 503
                            └─ 202 Accepted {orderId}
                                         │
                      orders.created ────┘
                            ▼
                      Worker(s): wasReleased? → exists? → charge (Opossum breaker) → BEGIN; orders+payments+events; COMMIT → ack
                            │ transient error → orders.retry.{0,1,2} (5s/30s/120s TTL, dead-letter back) → … → DLQ
                            │ breaker open    → delay queue WITHOUT consuming a retry
                            └ declined / exhausted → FAILED row + release_stock.lua (idempotent)
```

## Why it can't oversell

* `reserve_stock.lua` is one atomic Redis operation: check + decrement + idempotency record. Concurrent requests are serialized by Redis, not by SQL row locks.
* PostgreSQL never decrements a stock column (no hot row). Sold units are derived from the append-only `orders` table (`product_sales` view); worker writes are idempotent (`ON CONFLICT DO NOTHING`, `UNIQUE(order_id)`, `UNIQUE(user_id, product_id, idempotency_key)`).
* Redis outage ⇒ API returns 503 (fails closed); it never guesses stock.
* Compensation (`release_stock.lua`) is idempotent via a `released:` flag, and workers skip orders whose flag is set.

## Verify after a load test

```bash
# 1. never more confirmed units than stock (seed stock = 1000)
docker compose exec postgres psql -U flash flashsale -c \
  "SELECT status, count(*), sum(quantity) FROM orders GROUP BY status;"
# 2. Redis stock is never negative
docker compose exec redis redis-cli GET 'stock:{sku-flash-001}'
# 3. queues drained, DLQ inspected
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```
Invariant: `redis_stock + confirmed_units + in_flight_units = initial_stock`, and `confirmed_units ≤ 1000`.

## Useful PromQL (Grafana → Explore)

```promql
# latency percentiles (ingestion)
histogram_quantile(0.50, sum by (le) (rate(checkout_ingestion_duration_seconds_bucket[1m])))
histogram_quantile(0.95, sum by (le) (rate(checkout_ingestion_duration_seconds_bucket[1m])))
histogram_quantile(0.99, sum by (le) (rate(checkout_ingestion_duration_seconds_bucket[1m])))
# HTTP percentiles / throughput / status codes
histogram_quantile(0.99, sum by (le, route) (rate(http_request_duration_seconds_bucket[1m])))
sum by (status_code) (rate(http_requests_total{route="/api/v1/orders/checkout"}[30s]))
# Redis reservation hit/miss ("lock hits/misses")
sum by (result) (rate(stock_reservation_total[30s]))
# rate limiting
sum by (scope, result) (rate(rate_limit_decisions_total[30s]))
# queue depth, retry/DLQ pressure
rabbitmq_queue_depth
sum by (outcome) (rate(orders_processed_total[1m]))
# circuit breaker (0 closed, 1 half-open, 2 open)
circuit_breaker_state
```

## Runtime tuning

```bash
# retune the token buckets live, no deploy (cached ≤5s per API instance)
docker compose exec redis redis-cli HSET ratelimit:policy:global capacity 8000 refillPerSec 6000
docker compose exec redis redis-cli HSET ratelimit:policy:user   capacity 3    refillPerSec 0.5
# simulate a payment outage to watch the breaker open
PAYMENT_TRANSIENT_FAILURE_RATE=0.9 docker compose up -d worker
# scale: remove the api host-port mapping, front it with a load balancer, then
docker compose up -d --scale api=3 --scale worker=6     # Prometheus discovers replicas via DNS SD
```

## Known trade-offs (read before production)

1. **Dual write (Redis → RabbitMQ).** If the process dies between the Lua reservation and the publish confirm, units stay reserved with no event. The hardened version is an outbox: have `reserve_stock.lua` also `XADD` the event to a Redis Stream and let a relay publish to RabbitMQ. A periodic reconciler comparing `stock + confirmed + in-flight` to the initial stock is a good backstop either way.
2. **Redis durability.** AOF `everysec` can lose ~1s of writes on a crash. Use Redis with replicas/Sentinel or a managed HA tier, and reconcile on failover.
3. **Ambiguous payment outcome.** When retries are exhausted the stock is released but the card may have been charged; the gateway call is keyed by `orderId`, so a reconciliation/void job can resolve it from the DLQ.
4. **Auth.** `X-User-Id` is a stand-in; derive the user from a verified JWT.
5. **Queues.** Switch to quorum queues on a multi-node RabbitMQ cluster.
6. **Locust** is Python-bound: use `--processes -1` (multi-core) and run it on a separate machine for honest 2k+ rps numbers.
