import { env } from '../../config/env';

export interface ChargeRequest {
  orderId: string; // doubles as the gateway idempotency key: retries can never double-charge
  userId: string;
  amountCents: number;
}

export interface ChargeResult {
  paymentId: string;
  status: 'SUCCEEDED';
}

/** Business failure. Retrying will not help. Does NOT count against the circuit breaker. */
export class PaymentDeclinedError extends Error {
  constructor(message = 'payment declined') {
    super(message);
    this.name = 'PaymentDeclinedError';
  }
}

/** Infrastructure failure (5xx, network, timeout). Retriable. DOES count against the breaker. */
export class PaymentGatewayError extends Error {
  constructor(message = 'payment gateway unavailable') {
    super(message);
    this.name = 'PaymentGatewayError';
  }
}

export interface PaymentGateway {
  charge(req: ChargeRequest): Promise<ChargeResult>;
}

/** Stand-in for Stripe/Adyen/etc. Swap with a real adapter implementing PaymentGateway. */
export class SimulatedPaymentGateway implements PaymentGateway {
  async charge(req: ChargeRequest): Promise<ChargeResult> {
    await new Promise((r) => setTimeout(r, env.PAYMENT_LATENCY_MS));
    const roll = Math.random();
    if (roll < env.PAYMENT_TRANSIENT_FAILURE_RATE) throw new PaymentGatewayError('simulated 503 from gateway');
    if (roll < env.PAYMENT_TRANSIENT_FAILURE_RATE + env.PAYMENT_DECLINE_RATE) throw new PaymentDeclinedError('card declined');
    return { paymentId: `pay_${req.orderId}`, status: 'SUCCEEDED' };
  }
}
