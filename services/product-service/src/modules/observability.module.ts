import { Global, Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { HealthController } from '../health/health.controller.js';
import { MetricsController } from '../health/metrics.controller.js';
import { MetricsService } from '../platform/observability/metrics.service.js';

@Global()
@Module({
  imports: [TerminusModule],
  controllers: [HealthController, MetricsController],
  providers: [MetricsService],
  exports: [MetricsService],
})
export class ObservabilityModule {}
