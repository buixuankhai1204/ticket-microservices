import { Injectable, type NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { MetricsService } from '../../../platform/observability/metrics.service.js';

const UNMATCHED = 'unmatched';
const EXCLUDED_PATHS = new Set(['/healthz', '/readyz', '/metrics']);

@Injectable()
export class MetricsMiddleware implements NestMiddleware {
  constructor(private readonly metrics: MetricsService) {}

  use(request: Request, response: Response, next: NextFunction): void {
    const start = process.hrtime.bigint();
    response.on('finish', () => {
      const routePath: unknown = request.route?.path;
      const path =
        typeof routePath === 'string' && !routePath.includes('*')
          ? `${request.baseUrl}${routePath}`
          : UNMATCHED;
      if (EXCLUDED_PATHS.has(path)) {
        return;
      }
      const seconds = Number(process.hrtime.bigint() - start) / 1e9;
      this.metrics.observe(request.method, path, response.statusCode, seconds);
    });
    next();
  }
}
