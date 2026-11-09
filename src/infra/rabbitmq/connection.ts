import amqp from 'amqplib';
import type { Channel, ConfirmChannel } from 'amqplib';
import { logger } from '../../config/logger';

// amqplib's typings changed shape between minor versions; derive the type instead of naming it.
type Connection = Awaited<ReturnType<typeof amqp.connect>>;

/** Thin lifecycle wrapper: lazy connect, de-duplicated concurrent connects, close notification. */
export class RabbitConnection {
  private conn?: Connection;
  private connecting?: Promise<Connection>;

  constructor(
    private readonly url: string,
    private readonly onClose?: () => void,
  ) {}

  async connect(): Promise<Connection> {
    if (this.conn) return this.conn;
    if (!this.connecting) {
      this.connecting = amqp
        .connect(this.url)
        .then((c) => {
          c.on('error', (err: Error) => logger.error({ err }, 'rabbitmq connection error'));
          c.on('close', () => {
            logger.warn('rabbitmq connection closed');
            this.conn = undefined;
            this.connecting = undefined;
            this.onClose?.();
          });
          this.conn = c;
          return c;
        })
        .catch((err) => {
          this.connecting = undefined;
          throw err;
        });
    }
    return this.connecting;
  }

  async createChannel(): Promise<Channel> {
    return (await this.connect()).createChannel();
  }

  async createConfirmChannel(): Promise<ConfirmChannel> {
    return (await this.connect()).createConfirmChannel();
  }

  async close(): Promise<void> {
    const c = this.conn;
    this.conn = undefined;
    this.connecting = undefined;
    await c?.close().catch(() => undefined);
  }
}
