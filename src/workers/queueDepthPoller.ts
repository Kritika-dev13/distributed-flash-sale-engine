import type { Channel } from 'amqplib';
import { logger } from '../config/logger';
import { metrics } from '../infra/metrics';
import { ALL_QUEUE_NAMES } from '../infra/rabbitmq/topology';

/** Polls queue depth (messages ready) for Prometheus. Needs its own channel: a failed passive declare closes it. */
export function startQueueDepthPoller(channel: Channel, intervalMs = 5_000): () => void {
  const timer = setInterval(async () => {
    for (const queue of ALL_QUEUE_NAMES) {
      try {
        const { messageCount } = await channel.checkQueue(queue);
        metrics.queueDepth.set({ queue }, messageCount);
      } catch (err) {
        logger.warn({ err, queue }, 'queue depth poll failed');
        return;
      }
    }
  }, intervalMs);
  timer.unref();
  return () => clearInterval(timer);
}
