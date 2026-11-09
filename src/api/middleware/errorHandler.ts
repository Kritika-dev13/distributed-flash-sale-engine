import type { ErrorRequestHandler, RequestHandler } from 'express';
import { logger } from '../../config/logger';

export const notFound: RequestHandler = (_req, res) => {
  res.status(404).json({ code: 'NOT_FOUND' });
};

export const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  const type = (err as { type?: string } | null)?.type;
  if (type === 'entity.parse.failed' || type === 'entity.too.large') {
    res.status(400).json({ code: 'BAD_REQUEST', message: 'Malformed or oversized JSON body' });
    return;
  }
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ code: 'INTERNAL_ERROR' });
};
