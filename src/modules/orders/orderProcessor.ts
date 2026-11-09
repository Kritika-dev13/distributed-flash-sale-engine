import { logger } from '../../config/logger';
import type { InventoryService } from '../inventory/inventory.service';
import type { PaymentBreaker } from '../payment/paymentBreaker';
import { PaymentDeclinedError } from '../payment/paymentGateway';
import type { OrderCreatedData } from './order.events';
import type { OrderRepository } from './orders.repository';

export type ProcessOutcome = 'confirmed' | 'duplicate' | 'cancelled' | 'declined';

/**
 * Business workflow for one OrderCreated event. Throws on *retriable* failures;
 * the consumer decides how to retry. Safe to run more than once for the same order.
 */
export class OrderProcessor {
  constructor(
    private readonly repo: OrderRepository,
    private readonly inventory: InventoryService,
    private readonly payments: PaymentBreaker,
  ) {}

  async process(data: OrderCreatedData): Promise<ProcessOutcome> {
    // 1. Reservation already cancelled (e.g. API gave up after an ambiguous publish timeout)?
    if (await this.inventory.wasReleased(data.productId, data.orderId)) return 'cancelled';

    // 2. Already persisted (redelivery after a crash between COMMIT and ACK)?
    if (await this.repo.exists(data.orderId)) return 'duplicate';

    const unitPrice = await this.repo.getPriceCents(data.productId);
    if (unitPrice === null) {
      logger.error({ orderId: data.orderId, productId: data.productId }, 'unknown product');
      await this.fail(data, 'UNKNOWN_PRODUCT');
      return 'declined';
    }
    const amountCents = unitPrice * data.quantity;

    // 3. Charge. orderId is the gateway idempotency key, so a retry after a lost response is safe.
    let paymentId: string;
    try {
      ({ paymentId } = await this.payments.charge({
        orderId: data.orderId,
        userId: data.userId,
        amountCents,
      }));
    } catch (err) {
      if (err instanceof PaymentDeclinedError) {
        await this.fail(data, 'PAYMENT_DECLINED');
        return 'declined';
      }
      throw err; // transient / breaker-open -> consumer retry logic
    }

    // 4. Persist atomically. If this throws, the message is retried and step 3 is a no-op at the gateway.
    await this.repo.saveConfirmed({ data, amountCents, paymentId });
    return 'confirmed';
  }

  /** Terminal failure: record it and return the units to Redis stock (idempotent). */
  async fail(data: OrderCreatedData, reason: string): Promise<void> {
    await this.repo.saveFailed(data, reason);
    await this.inventory.release({
      productId: data.productId,
      userId: data.userId,
      orderId: data.orderId,
      idempotencyKey: data.idempotencyKey,
      quantity: data.quantity,
    });
  }
}
