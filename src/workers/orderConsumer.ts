import type { Channel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';
import { env } from '../config/env';
import { logger } from '../config/logger';
import { metrics } from '../infra/metrics';
import type { RabbitConnection } from '../infra/rabbitmq/connection';
import {
  assertTopology,
  EXCHANGES,
  QUEUES,
  RETRY_DELAYS_MS,
  ROUTING_KEYS,
} from '../infra/rabbitmq/topology';
import { PaymentBreaker } from '../modules/payment/paymentBreaker';
import { OrderCreatedSchema, type OrderCreatedEvent } from '../modules/orders/order.events';
import type { OrderProcessor } from '../modules/orders/orderProcessor';
import { withTimeout } from '../shared/withTimeout';

/**
 * Consumer with manual acks, bounded concurrency (prefetch) and a three-way failure policy:
 *
 *  | failure                          | action                                                       |
 *  |----------------------------------|--------------------------------------------------------------|
 *  | malformed message                | straight to DLQ                                              |
 *  | payment declined (business)      | OrderProcessor persists FAILED + releases stock, ack         |
 *  | circuit breaker OPEN             | delay-queue WITHOUT burning a retry attempt (capped)         |
 *  | any other error (DB, 5xx, timeout)| delay-queue tier 5s -> 30s -> 120s, then DLQ (stock released)|
 *
 * A message is acked only AFTER its replacement (retry/DLQ copy) is confirmed by the broker,
 * so there is no point at which it can be lost. If even that fails we nack(requeue=true).
 */
export class OrderConsumer {
  private consumeChannel?: Channel;
  private publishChannel?: ConfirmChannel;
  private consumerTag?: string;
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    private readonly rabbit: RabbitConnection,
    private readonly processor: OrderProcessor,
  ) {}

  async start(): Promise<void> {
    this.publishChannel = await this.rabbit.createConfirmChannel();
    this.consumeChannel = await this.rabbit.createChannel();
    await assertTopology(this.consumeChannel);
    await this.consumeChannel.prefetch(env.WORKER_PREFETCH);

    const { consumerTag } = await this.consumeChannel.consume(
      QUEUES.created,
      (msg) => {
        if (!msg) return; // consumer cancelled by broker
        const p: Promise<void> = this.handle(msg).finally(() => this.inFlight.delete(p));
        this.inFlight.add(p);
      },
      { noAck: false },
    );
    this.consumerTag = consumerTag;
    logger.info({ queue: QUEUES.created, prefetch: env.WORKER_PREFETCH }, 'order consumer started');
  }

  /** Graceful stop: stop taking new work, let in-flight messages finish and ack. */
  async stop(): Promise<void> {
    if (this.consumeChannel && this.consumerTag) {
      await this.consumeChannel.cancel(this.consumerTag).catch(() => undefined);
    }
    await Promise.allSettled([...this.inFlight]);
    await this.consumeChannel?.close().catch(() => undefined);
    await this.publishChannel?.close().catch(() => undefined);
  }

  private async handle(msg: ConsumeMessage): Promise<void> {
    const ch = this.consumeChannel as Channel;
    const stopTimer = metrics.orderProcessingDuration.startTimer();

    try {
      const headers = msg.properties.headers ?? {};
      const attempt = Number(headers['x-attempt'] ?? 0);
      const deferrals = Number(headers['x-deferrals'] ?? 0);

      const event = this.parse(msg);
      let outcome: string;

      if (!event) {
        await this.deadLetter(msg, 'MALFORMED_MESSAGE');
        outcome = 'malformed';
      } else {
        try {
          outcome = await this.processor.process(event.data);
        } catch (err) {
          outcome = await this.onFailure(msg, event, err, attempt, deferrals);
        }
      }

      ch.ack(msg);
      metrics.ordersProcessed.inc({ outcome });
    } catch (fatal) {
      // We could not even hand the message off safely: put it back, never drop it.
      logger.error({ err: fatal, messageId: msg.properties.messageId }, 'handler failed, requeueing');
      try {
        (this.consumeChannel as Channel).nack(msg, false, true);
      } catch {
        /* channel is gone; the broker redelivers unacked messages automatically */
      }
    } finally {
      stopTimer();
    }
  }

  private parse(msg: ConsumeMessage): OrderCreatedEvent | null {
    try {
      const parsed = OrderCreatedSchema.safeParse(JSON.parse(msg.content.toString('utf8')));
      return parsed.success ? parsed.data : null;
    } catch {
      return null;
    }
  }

  private async onFailure(
    msg: ConsumeMessage,
    event: OrderCreatedEvent,
    err: unknown,
    attempt: number,
    deferrals: number,
  ): Promise<string> {
    const lastError = err instanceof Error ? err.message : String(err);

    // Dependency is known-down: park the message without consuming one of its retries.
    if (PaymentBreaker.isOpenError(err) && deferrals < env.MAX_BREAKER_DEFERRALS) {
      const tier = Math.min(attempt, RETRY_DELAYS_MS.length - 1);
      await this.publishToRetry(msg, tier, { attempt, deferrals: deferrals + 1, lastError });
      return 'deferred';
    }

    if (attempt < RETRY_DELAYS_MS.length) {
      await this.publishToRetry(msg, attempt, { attempt: attempt + 1, deferrals, lastError });
      logger.warn({ orderId: event.data.orderId, attempt: attempt + 1, lastError }, 'order scheduled for retry');
      return 'retried';
    }

    // Exhausted. Record + release stock, then park for humans. (If the gateway may have charged the card,
    // a reconciliation job keyed on orderId should void it: see README "Known trade-offs".)
    logger.error({ orderId: event.data.orderId, lastError }, 'retries exhausted, dead-lettering');
    await this.processor.fail(event.data, 'RETRIES_EXHAUSTED');
    await this.deadLetter(msg, `RETRIES_EXHAUSTED: ${lastError}`);
    return 'dead_lettered';
  }

  /** Default exchange + queue name as routing key => straight into the TTL'd delay queue. */
  private publishToRetry(
    msg: ConsumeMessage,
    tier: number,
    h: { attempt: number; deferrals: number; lastError: string },
  ): Promise<void> {
    return this.publishConfirmed('', QUEUES.retry(tier), msg, {
      'x-attempt': h.attempt,
      'x-deferrals': h.deferrals,
      'x-last-error': h.lastError.slice(0, 500),
    });
  }

  private deadLetter(msg: ConsumeMessage, reason: string): Promise<void> {
    return this.publishConfirmed(EXCHANGES.dlx, ROUTING_KEYS.dead, msg, {
      'x-attempt': Number(msg.properties.headers?.['x-attempt'] ?? 0),
      'x-dead-reason': reason.slice(0, 500),
    });
  }

  private publishConfirmed(
    exchange: string,
    routingKey: string,
    original: ConsumeMessage,
    headers: Record<string, string | number>,
  ): Promise<void> {
    const ch = this.publishChannel as ConfirmChannel;
    const options: Options.Publish = {
      persistent: true,
      contentType: original.properties.contentType ?? 'application/json',
      messageId: original.properties.messageId,
      type: original.properties.type,
      timestamp: original.properties.timestamp,
      headers, // fresh headers only: we deliberately do not copy broker-managed x-death
    };
    return withTimeout(
      new Promise<void>((resolve, reject) => {
        ch.publish(exchange, routingKey, original.content, options, (err) => (err ? reject(err) : resolve()));
      }),
      env.PUBLISH_TIMEOUT_MS,
      'rabbitmq republish confirm',
    );
  }
}
