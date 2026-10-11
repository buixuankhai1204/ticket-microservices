import { Module, type DynamicModule } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { BookingEventsController } from '../adapter/messaging/kafka/booking-events.controller.js';
import { BookingEventsProcessor } from '../adapter/messaging/kafka/booking-events.processor.js';
import { ConsumerLifecycle } from '../adapter/messaging/kafka/consumer-lifecycle.js';
import { ConsumerPolicy } from '../adapter/messaging/kafka/consumer-policy.js';
import { DeadLetterPublisher } from '../adapter/messaging/kafka/dead-letter.publisher.js';
import { KafkaDeadLetterPublisher } from '../adapter/messaging/kafka/kafka-dead-letter.publisher.js';
import { appConfig } from '../platform/config/app.config.js';
import { OrdersModule } from './orders.module.js';

const BASE_DELAY_MS = 250;
const MAX_DELAY_MS = 30000;

@Module({})
export class MessagingModule {
  static forRoot(): DynamicModule {
    if (process.env.KAFKA_CONSUMER_ENABLED === 'false') {
      return { module: MessagingModule };
    }
    return {
      module: MessagingModule,
      imports: [OrdersModule],
      controllers: [BookingEventsController],
      providers: [
        BookingEventsProcessor,
        ConsumerLifecycle,
        { provide: DeadLetterPublisher, useClass: KafkaDeadLetterPublisher },
        {
          provide: ConsumerPolicy,
          inject: [appConfig.KEY],
          useFactory: (
            config: ConfigType<typeof appConfig>,
          ): ConsumerPolicy => ({
            maxAttempts: config.kafka.maxAttempts,
            baseDelayMs: BASE_DELAY_MS,
            maxDelayMs: MAX_DELAY_MS,
          }),
        },
      ],
    };
  }
}
