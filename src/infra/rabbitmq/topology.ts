import type { Channel } from 'amqplib';

/**
 * Topology
 *
 *  API --publish--> [orders.exchange] --orders.created--> (orders.created) --> worker
 *
 *  On transient failure the worker re-publishes to a delay queue:
 *     (orders.retry.0 / 5s)  (orders.retry.1 / 30s)  (orders.retry.2 / 120s)
 *  Those queues have a TTL and dead-letter BACK into [orders.exchange] => automatic delayed retry.
 *
 *  Poison messages / exhausted retries --> [orders.dlx] --orders.dead--> (orders.dlq)
 *
 * Both the API and the worker call assertTopology() at boot; the declarations are idempotent.
 * For HA clusters, switch the queues to `x-queue-type: quorum`.
 */
export const EXCHANGES = { main: 'orders.exchange', dlx: 'orders.dlx' } as const;
export const ROUTING_KEYS = { created: 'orders.created', dead: 'orders.dead' } as const;

export const RETRY_DELAYS_MS = [5_000, 30_000, 120_000] as const;

export const QUEUES = {
  created: 'orders.created',
  dlq: 'orders.dlq',
  retry: (tier: number) => `orders.retry.${tier}`,
} as const;

export const ALL_QUEUE_NAMES: string[] = [
  QUEUES.created,
  QUEUES.dlq,
  ...RETRY_DELAYS_MS.map((_, i) => QUEUES.retry(i)),
];

export async function assertTopology(ch: Channel): Promise<void> {
  await ch.assertExchange(EXCHANGES.main, 'direct', { durable: true });
  await ch.assertExchange(EXCHANGES.dlx, 'direct', { durable: true });

  // Main work queue. Anything nack'd with requeue=false lands in the DLQ.
  await ch.assertQueue(QUEUES.created, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.dlx,
      'x-dead-letter-routing-key': ROUTING_KEYS.dead,
    },
  });
  await ch.bindQueue(QUEUES.created, EXCHANGES.main, ROUTING_KEYS.created);

  // Final resting place for messages that need a human / replay tool.
  await ch.assertQueue(QUEUES.dlq, { durable: true });
  await ch.bindQueue(QUEUES.dlq, EXCHANGES.dlx, ROUTING_KEYS.dead);

  // Delay tiers: messages sit until the TTL expires, then dead-letter back to the main queue.
  for (let tier = 0; tier < RETRY_DELAYS_MS.length; tier++) {
    await ch.assertQueue(QUEUES.retry(tier), {
      durable: true,
      arguments: {
        'x-message-ttl': RETRY_DELAYS_MS[tier],
        'x-dead-letter-exchange': EXCHANGES.main,
        'x-dead-letter-routing-key': ROUTING_KEYS.created,
      },
    });
  }
}
