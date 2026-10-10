import { Controller, Get, Res, VERSION_NEUTRAL } from '@nestjs/common';
import { ApiExcludeController } from '@nestjs/swagger';
import type { Response } from 'express';
import { MetricsService } from '../platform/observability/metrics.service.js';

@ApiExcludeController()
@Controller({ version: VERSION_NEUTRAL })
export class MetricsController {
  constructor(private readonly metrics: MetricsService) {}

  @Get('metrics')
  async scrape(
    @Res({ passthrough: true }) response: Response,
  ): Promise<string> {
    response.setHeader('Content-Type', this.metrics.contentType);
    return this.metrics.scrape();
  }
}
