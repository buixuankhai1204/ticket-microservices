import {
  Module,
  ValidationPipe,
  type MiddlewareConsumer,
  type NestModule,
} from '@nestjs/common';
import { ConfigModule, type ConfigType } from '@nestjs/config';
import { APP_FILTER, APP_PIPE } from '@nestjs/core';
import { LoggerModule } from 'nestjs-pino';
import { AllExceptionsFilter } from './adapter/http/filters/all-exceptions.filter.js';
import { MetricsMiddleware } from './adapter/http/middleware/metrics.middleware.js';
import { CatalogModule } from './modules/catalog.module.js';
import { DatabaseModule } from './modules/database.module.js';
import { ObservabilityModule } from './modules/observability.module.js';
import { appConfig } from './platform/config/app.config.js';
import { buildLoggerParams } from './platform/logger/logger.options.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, load: [appConfig] }),
    LoggerModule.forRootAsync({
      inject: [appConfig.KEY],
      useFactory: (config: ConfigType<typeof appConfig>) =>
        buildLoggerParams(config),
    }),
    DatabaseModule,
    ObservabilityModule,
    CatalogModule,
  ],
  providers: [
    {
      provide: APP_PIPE,
      useValue: new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    },
    { provide: APP_FILTER, useClass: AllExceptionsFilter },
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(MetricsMiddleware).forRoutes('*');
  }
}
