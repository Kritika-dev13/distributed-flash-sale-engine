import { z } from 'zod';
import { env } from '../../config/env';

const SAFE_ID = /^[A-Za-z0-9_-]+$/;

export const checkoutHeadersSchema = z.object({
  'idempotency-key': z.string().min(8).max(128).regex(SAFE_ID),
  'x-user-id': z.string().min(1).max(64).regex(SAFE_ID), // stand-in for a verified JWT subject
});

export const checkoutBodySchema = z
  .object({
    productId: z.string().min(1).max(64).regex(SAFE_ID),
    quantity: z.number().int().min(1).max(env.MAX_QUANTITY_PER_ORDER).default(1),
  })
  .strict();

export type CheckoutBody = z.infer<typeof checkoutBodySchema>;
