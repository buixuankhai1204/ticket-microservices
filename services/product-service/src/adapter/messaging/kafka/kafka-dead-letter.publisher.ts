import {
  Inject,
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { Kafka, type Producer } from 'kafkajs';
import { appConfig } from '../../../platform/config/app.config.js';
import {
  DeadLetterPublisher,
  type DeadLetterRecord,
} from './dead-letter.publisher.js';
import { CLIENT_ID } from './topics.js';

@Injectable()
export class KafkaDeadLetterPublisher
  extends DeadLetterPublisher
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly producer: Producer;

  constructor(@Inject(appConfig.KEY) config: ConfigType<typeof appConfig>) {
    super();
    this.producer = new Kafka({
      clientId: `${CLIENT_ID}-dlq`,
      brokers: config.kafka.brokers,
    }).producer({ allowAutoTopicCreation: false });
  }

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  async publish(record: DeadLetterRecord): Promise<void> {
    await this.producer.send({
      topic: `${record.sourceTopic}.dlq`,
      acks: -1,
      messages: [
        {
          key: record.key,
          value: record.value,
          headers: {
            ...record.headers,
            'x-dlq-reason': record.reason,
            'x-dlq-source-topic': record.sourceTopic,
            'x-dlq-source-partition': String(record.sourcePartition),
            'x-dlq-source-offset': record.sourceOffset,
          },
        },
      ],
    });
  }
}
