import type { RequestHandler } from 'express';
import { metrics } from '../../infra/metrics';

/** Records latency histogram + status-code counter for every matched route. */
export const httpMetrics: RequestHandler = (req, res, next) => {
  const stop = metrics.httpRequestDuration.startTimer();
  res.on('finish', () => {
    const route = req.route?.path ? `${req.baseUrl}${req.route.path}` : 'unmatched';
    const labels = { method: req.method, route, status_code: String(res.statusCode) };
    stop(labels);
    metrics.httpRequestsTotal.inc(labels);
  });
  next();
};
