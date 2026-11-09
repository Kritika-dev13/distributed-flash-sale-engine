import CircuitBreaker from 'opossum';
import { env } from '../../config/env';
import { logger } from '../../config/logger';
import { metrics } from '../../infra/metrics';
import {
  PaymentDeclinedError,
  type ChargeRequest,
  type ChargeResult,
  type PaymentGateway,
} from './paymentGateway';

const NAME = 'payment-gateway';

/**
 * Circuit breaker around the payment gateway (Opossum).
 *   CLOSED    -> calls flow; failures are tracked in a rolling window
 *   OPEN      -> once >= errorThresholdPercentage (and >= volumeThreshold calls) fail, calls are rejected
 *                instantly with code EOPENBREAKER: no threads/sockets wasted on a dying dependency
 *   HALF-OPEN -> after resetTimeout one probe call is allowed; success closes, failure re-opens
 * Card declines are business outcomes, not outages, so errorFilter keeps them out of the failure stats.
 */
export class PaymentBreaker {
  private readonly breaker: CircuitBreaker<[ChargeRequest], ChargeResult>;

  constructor(gateway: PaymentGateway) {
    this.breaker = new CircuitBreaker<[ChargeRequest], ChargeResult>((req) => gateway.charge(req), {
      name: NAME,
      timeout: env.PAYMENT_TIMEOUT_MS,
      errorThresholdPercentage: env.BREAKER_ERROR_THRESHOLD_PCT,
      resetTimeout: env.BREAKER_RESET_MS,
      volumeThreshold: env.BREAKER_VOLUME_THRESHOLD,
      rollingCountTimeout: 10_000,
      rollingCountBuckets: 10,
      errorFilter: (err: unknown) => err instanceof PaymentDeclinedError,
    });

    metrics.circuitBreakerState.set({ name: NAME }, 0);
    this.breaker.on('open', () => {
      metrics.circuitBreakerState.set({ name: NAME }, 2);
      logger.warn('payment circuit OPEN');
    });
    this.breaker.on('halfOpen', () => {
      metrics.circuitBreakerState.set({ name: NAME }, 1);
      logger.info('payment circuit HALF-OPEN');
    });
    this.breaker.on('close', () => {
      metrics.circuitBreakerState.set({ name: NAME }, 0);
      logger.info('payment circuit CLOSED');
    });
    
    const events = ['success', 'failure', 'timeout', 'reject', 'short-circuit'] as const;
    for (const event of events) {
      (this.breaker.on as any)(event, () => metrics.circuitBreakerEvents.inc({ name: NAME, event }));
    }
  }

  charge(req: ChargeRequest): Promise<ChargeResult> {
    return this.breaker.fire(req);
  }

  /** True when the call was rejected because the circuit is open (no request reached the gateway). */
  static isOpenError(err: unknown): boolean {
    return (err as { code?: string } | null)?.code === 'EOPENBREAKER';
  }

  shutdown(): void {
    this.breaker.shutdown();
  }
}