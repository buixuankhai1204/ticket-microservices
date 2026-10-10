import { VersioningType } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import helmet from 'helmet';
import { Logger } from 'nestjs-pino';

const JSON_BODY_LIMIT = '100kb';
const UNPREFIXED_ROUTES = ['healthz', 'readyz', 'metrics'];

export function configureApp(app: NestExpressApplication): void {
  app.useLogger(app.get(Logger));
  app.use(helmet());
  app.useBodyParser('json', { limit: JSON_BODY_LIMIT });
  app.setGlobalPrefix('api', { exclude: UNPREFIXED_ROUTES });
  app.enableVersioning({ type: VersioningType.URI, defaultVersion: '1' });
  app.enableShutdownHooks();
}

export function setupSwagger(app: NestExpressApplication): void {
  const document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle('product-service')
      .setDescription('Food, drink and add-on catalog with per-event stock')
      .setVersion('1')
      .addBearerAuth()
      .build(),
  );
  SwaggerModule.setup('swagger', app, document);
}
