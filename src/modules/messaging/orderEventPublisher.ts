import type { ConfirmChannel } from 'amqplib';
import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { metrics } from '../../infra/metrics';
import type { RabbitConnection } from '../../infra/rabbitmq/connection';
import { assertTopology, EXCHANGES, ROUTING_KEYS } from '../../infra/rabbitmq/topology';
import { withTimeout } from '../../shared/withTimeout';
import type { OrderCreatedEvent } from '../orders/order.events';

/**
 * RabbitMQ producer.
 *  - Uses a CONFIRM channel: the promise resolves only after the broker has persisted the message.
 *  - Persistent messages (delivery-mode 2) on a durable queue survive a broker restart.
 *  - Lazily re-opens its channel, so a broker blip degrades to a few 503s instead of a dead API.
 */
export class OrderEventPublisher {
  private channel?: ConfirmChannel;
  private opening?: Promise<ConfirmChannel>;

  constructor(private readonly rabbit: RabbitConnection) {}

  async init(): Promise<void> {
    await this.getChannel();
  }

  isReady(): boolean {
    return this.channel !== undefined;
  }

  private getChannel(): Promise<ConfirmChannel> {
    if (this.channel) return Promise.resolve(this.channel);
    this.opening ??= (async () => {
      const ch = await this.rabbit.createConfirmChannel();
      await assertTopology(ch);
      ch.on('error', (err: Error) => logger.error({ err }, 'publisher channel error'));
      ch.on('close', () => {
        this.channel = undefined;
        this.opening = undefined;
      });
      this.channel = ch;
      return ch;
    })().catch((err) => {
      this.opening = undefined;
      throw err;
    });
    return this.opening;
  }

  /** Resolves when the broker confirms; rejects on nack, channel failure or timeout. */
  async publishOrderCreated(event: OrderCreatedEvent): Promise<void> {
    try {
      const ch = await this.getChannel();
      const body = Buffer.from(JSON.stringify(event));

      await withTimeout(
        new Promise<void>((resolve, reject) => {
          ch.publish(
            EXCHANGES.main,
            ROUTING_KEYS.created,
            body,
            {
              persistent: true,
              contentType: 'application/json',
              type: event.eventType,
              messageId: event.data.orderId, // stable id: consumers dedupe on it
              timestamp: Date.now(),
              headers: { 'x-attempt': 0 },
            },
            (err) => (err ? reject(err) : resolve()),
          );
        }),
        env.PUBLISH_TIMEOUT_MS,
        'rabbitmq publish confirm',
      );
      metrics.rabbitmqPublish.inc({ result: 'confirmed' });
    } catch (err) {
      metrics.rabbitmqPublish.inc({ result: 'failed' });
      throw err;
    }
  }

  async close(): Promise<void> {
    await this.channel?.close().catch(() => undefined);
    this.channel = undefined;
  }
}
