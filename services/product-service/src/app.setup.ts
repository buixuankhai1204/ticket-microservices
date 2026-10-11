import { VersioningType } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import helmet from 'helmet';
import { Logger } from 'nestjs-pino';
import { CLIENT_ID, CONSUMER_GROUP } from './adapter/messaging/kafka/topics.js';
import type { appConfig } from './platform/config/app.config.js';

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

export function connectKafkaConsumer(
  app: NestExpressApplication,
  config: ConfigType<typeof appConfig>,
): void {
  if (!config.kafka.enabled) {
    return;
  }
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: { clientId: CLIENT_ID, brokers: config.kafka.brokers },
      consumer: {
        groupId: `${CONSUMER_GROUP}${config.kafka.groupSuffix}`,
        sessionTimeout: 60000,
        heartbeatInterval: 3000,
        allowAutoTopicCreation: false,
      },
      subscribe: { fromBeginning: true },
      run: { autoCommit: false },
      parser: { keepBinary: true },
      postfixId: '',
    },
  });
}
