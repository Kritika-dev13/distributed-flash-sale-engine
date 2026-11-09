import { randomUUID } from 'node:crypto';
import { z } from 'zod';

/** Versioned event contract shared by producer and consumer. */
export const OrderCreatedSchema = z.object({
  eventId: z.string().uuid(),
  eventType: z.literal('OrderCreated'),
  version: z.literal(1),
  occurredAt: z.string().datetime(),
  data: z.object({
    orderId: z.string().uuid(),
    userId: z.string().min(1),
    productId: z.string().min(1),
    quantity: z.number().int().positive(),
    idempotencyKey: z.string().min(1),
  }),
});

export type OrderCreatedEvent = z.infer<typeof OrderCreatedSchema>;
export type OrderCreatedData = OrderCreatedEvent['data'];

export function buildOrderCreated(data: OrderCreatedData): OrderCreatedEvent {
  return {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    version: 1,
    occurredAt: new Date().toISOString(),
    data,
  };
}
