import { randomUUID } from 'node:crypto';
import {
  KafkaContainer,
  type StartedKafkaContainer,
} from '@testcontainers/kafka';
import type { ConfigType } from '@nestjs/config';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import {
  Kafka,
  logLevel,
  type Admin,
  type Consumer,
  type Producer,
} from 'kafkajs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RepositoryError, ValidationError } from '../src/domain/errors.js';
import type { DeadLetterRecord } from '../src/adapter/messaging/kafka/dead-letter.publisher.js';
import type { appConfig } from '../src/platform/config/app.config.js';
import type { BookingConfirmedInput } from '../src/usecase/record-booking-confirmed.usecase.js';

const SUFFIX = randomUUID().replaceAll('-', '').slice(0, 8);
const TOPIC = `booking.events.it${SUFFIX}`;
const DLQ_TOPIC = `${TOPIC}.dlq`;
const GROUP_SUFFIX = `-it-${SUFFIX}`;
process.env.KAFKA_BOOKING_EVENTS_TOPIC = TOPIC;

type Behaviour = (
  input: BookingConfirmedInput,
) => Promise<{ duplicate: boolean }>;

class ProgrammableUseCase {
  calls: BookingConfirmedInput[] = [];
  behaviour: Behaviour = async () => ({ duplicate: false });

  async execute(input: BookingConfirmedInput) {
    this.calls.push(input);
    return this.behaviour(input);
  }

  callsFor(eventId: string): number {
    return this.calls.filter((call) => call.eventId === eventId).length;
  }
}

let container: StartedKafkaContainer | undefined;
let app: NestExpressApplication;
let admin: Admin;
let producer: Producer;
let dlqConsumer: Consumer;
let publisher: { disconnect(): Promise<void> };
const confirmed = new ProgrammableUseCase();
const cancelled = new ProgrammableUseCase();
const deadLettered: Array<{
  value: string | null;
  headers: Record<string, string>;
}> = [];
let dlqFailures = 0;
let brokers: string[];

async function waitFor<T>(
  probe: () => T | Promise<T>,
  description: string,
  timeoutMs = 60000,
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await probe();
    if (result) {
      return result as NonNullable<T>;
    }
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

async function committedOffset(): Promise<number> {
  const groupId = `product-service-booking-events${GROUP_SUFFIX}`;
  const result = await admin.fetchOffsets({ groupId, topics: [TOPIC] });
  const offset = result[0]?.partitions[0]?.offset ?? '-1';
  return Number(offset);
}

async function produce(
  eventType: string,
  payload: string,
  eventId = randomUUID(),
): Promise<{ eventId: string; offset: number }> {
  const [metadata] = await producer.send({
    topic: TOPIC,
    messages: [
      {
        key: randomUUID(),
        value: payload,
        headers: { event_type: eventType, event_id: eventId },
      },
    ],
  });
  return { eventId, offset: Number(metadata.baseOffset) };
}

function bookingPayload(eventId: string, bookingId = randomUUID()): string {
  return JSON.stringify({
    event_id: eventId,
    booking_id: bookingId,
    user_id: randomUUID(),
    ticketed_event_id: randomUUID(),
    seat_ids: [randomUUID()],
    occurred_at: new Date().toISOString(),
  });
}

function deadLetterFor(offset: number) {
  return deadLettered.find(
    (record) => record.headers['x-dlq-source-offset'] === String(offset),
  );
}

beforeAll(async () => {
  const configured = process.env.TEST_KAFKA_BROKERS;
  if (configured) {
    brokers = configured.split(',');
  } else {
    container = await new KafkaContainer('confluentinc/cp-kafka:7.3.2')
      .withKraft()
      .start();
    brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`];
  }
  const kafka = new Kafka({
    clientId: 'it',
    brokers,
    logLevel: logLevel.NOTHING,
  });
  admin = kafka.admin();
  await admin.connect();
  for (const topic of [TOPIC, DLQ_TOPIC]) {
    if (container) {
      const created = await container.exec([
        'kafka-topics',
        '--bootstrap-server',
        'localhost:9092',
        '--create',
        '--topic',
        topic,
        '--partitions',
        '1',
        '--replication-factor',
        '1',
      ]);
      if (created.exitCode !== 0) {
        throw new Error(`could not create ${topic}: ${created.output}`);
      }
    } else {
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] });
    }
  }
  producer = kafka.producer();
  await producer.connect();

  dlqConsumer = kafka.consumer({ groupId: `dlq-reader-${SUFFIX}` });
  await dlqConsumer.connect();
  await dlqConsumer.subscribe({ topic: DLQ_TOPIC, fromBeginning: true });
  await dlqConsumer.run({
    eachMessage: async ({ message }) => {
      const headers: Record<string, string> = {};
      for (const [name, value] of Object.entries(message.headers ?? {})) {
        headers[name] = String(value);
      }
      deadLettered.push({ value: message.value?.toString() ?? null, headers });
    },
  });

  const { KafkaDeadLetterPublisher } =
    await import('../src/adapter/messaging/kafka/kafka-dead-letter.publisher.js');
  const { DeadLetterPublisher } =
    await import('../src/adapter/messaging/kafka/dead-letter.publisher.js');
  const { BookingEventsController } =
    await import('../src/adapter/messaging/kafka/booking-events.controller.js');
  const { BookingEventsProcessor } =
    await import('../src/adapter/messaging/kafka/booking-events.processor.js');
  const { ConsumerLifecycle } =
    await import('../src/adapter/messaging/kafka/consumer-lifecycle.js');
  const { ConsumerPolicy } =
    await import('../src/adapter/messaging/kafka/consumer-policy.js');
  const { RecordBookingConfirmedUseCase } =
    await import('../src/usecase/record-booking-confirmed.usecase.js');
  const { HandleBookingCancelledUseCase } =
    await import('../src/usecase/handle-booking-cancelled.usecase.js');
  const { connectKafkaConsumer } = await import('../src/app.setup.js');

  const config = {
    kafka: {
      enabled: true,
      brokers,
      groupSuffix: GROUP_SUFFIX,
      maxAttempts: 3,
    },
  } as unknown as ConfigType<typeof appConfig>;
  const real = new KafkaDeadLetterPublisher(config);
  await real.onModuleInit();
  publisher = { disconnect: () => real.onApplicationShutdown() };
  const flaky = {
    async publish(record: DeadLetterRecord) {
      if (dlqFailures > 0) {
        dlqFailures -= 1;
        throw new Error('dead-letter topic unavailable');
      }
      await real.publish(record);
    },
  };

  const moduleRef = await Test.createTestingModule({
    controllers: [BookingEventsController],
    providers: [
      BookingEventsProcessor,
      ConsumerLifecycle,
      {
        provide: ConsumerPolicy,
        useValue: { maxAttempts: 3, baseDelayMs: 20, maxDelayMs: 50 },
      },
      { provide: DeadLetterPublisher, useValue: flaky },
      { provide: RecordBookingConfirmedUseCase, useValue: confirmed },
      { provide: HandleBookingCancelledUseCase, useValue: cancelled },
    ],
  }).compile();
  app = moduleRef.createNestApplication<NestExpressApplication>();
  connectKafkaConsumer(app, config);
  await app.startAllMicroservices();
  await app.init();
});

afterAll(async () => {
  await app?.close();
  await dlqConsumer?.disconnect();
  await producer?.disconnect();
  await admin?.disconnect();
  await publisher?.disconnect();
  await container?.stop();
});

describe('booking events consumer', () => {
  it('commits the offset only after the use case has run', async () => {
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    confirmed.behaviour = async () => {
      await gate;
      return { duplicate: false };
    };
    const sent = await produce(
      'BookingConfirmed',
      bookingPayload(randomUUID()),
    );
    await waitFor(
      () => confirmed.callsFor(sent.eventId) > 0,
      'the use case call',
    );
    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect(await committedOffset()).toBeLessThanOrEqual(sent.offset);

    release();
    await waitFor(
      async () => (await committedOffset()) === sent.offset + 1,
      'the offset commit',
    );
    confirmed.behaviour = async () => ({ duplicate: false });
  });

  it('skips an event type it does not handle but still commits it', async () => {
    const before = confirmed.calls.length + cancelled.calls.length;
    const sent = await produce(
      'BookingRequested',
      bookingPayload(randomUUID()),
    );
    await waitFor(
      async () => (await committedOffset()) === sent.offset + 1,
      'the offset commit',
    );
    expect(confirmed.calls.length + cancelled.calls.length).toBe(before);
  });

  it('routes a BookingCancelled to its own use case', async () => {
    const sent = await produce(
      'BookingCancelled',
      bookingPayload(randomUUID()),
    );
    await waitFor(
      () => cancelled.callsFor(sent.eventId) === 1,
      'the cancelled call',
    );
    await waitFor(
      async () => (await committedOffset()) === sent.offset + 1,
      'the offset commit',
    );
  });

  it('dead-letters a poison message with its origin and carries on', async () => {
    const poison = await produce('BookingConfirmed', 'this is not json');
    const next = await produce(
      'BookingConfirmed',
      bookingPayload(randomUUID()),
    );
    const record = await waitFor(
      () => deadLetterFor(poison.offset),
      'the dead-letter record',
    );
    expect(record.value).toBe('this is not json');
    expect(record.headers['x-dlq-reason']).toContain('poison');
    expect(record.headers['x-dlq-source-topic']).toBe(TOPIC);
    expect(record.headers['x-dlq-source-partition']).toBe('0');
    await waitFor(
      async () => (await committedOffset()) === next.offset + 1,
      'the offset commit past the poison message',
    );
    expect(confirmed.calls.length).toBeGreaterThan(0);
  });

  it('dead-letters a permanent failure at once without retrying', async () => {
    const eventId = randomUUID();
    confirmed.behaviour = async (input) => {
      if (input.eventId === eventId) {
        throw new ValidationError('rejected for good');
      }
      return { duplicate: false };
    };
    const sent = await produce(
      'BookingConfirmed',
      bookingPayload(eventId),
      eventId,
    );
    const record = await waitFor(
      () => deadLetterFor(sent.offset),
      'the dead-letter record',
    );
    expect(record.headers['x-dlq-reason']).toContain('permanent');
    expect(confirmed.callsFor(eventId)).toBe(1);
    await waitFor(
      async () => (await committedOffset()) === sent.offset + 1,
      'the offset commit',
    );
  });

  it('retries a transient failure and commits once it succeeds', async () => {
    const eventId = randomUUID();
    let failures = 2;
    confirmed.behaviour = async (input) => {
      if (input.eventId === eventId && failures > 0) {
        failures -= 1;
        throw new RepositoryError('deadlock detected', '40P01');
      }
      return { duplicate: false };
    };
    const sent = await produce(
      'BookingConfirmed',
      bookingPayload(eventId),
      eventId,
    );
    await waitFor(
      async () => (await committedOffset()) === sent.offset + 1,
      'the offset commit',
    );
    expect(confirmed.callsFor(eventId)).toBe(3);
    expect(deadLetterFor(sent.offset)).toBeUndefined();
  });

  it('dead-letters a message whose retries never succeed, with the attempt count', async () => {
    const eventId = randomUUID();
    confirmed.behaviour = async (input) => {
      if (input.eventId === eventId) {
        throw new RepositoryError('database unavailable');
      }
      return { duplicate: false };
    };
    const sent = await produce(
      'BookingConfirmed',
      bookingPayload(eventId),
      eventId,
    );
    const record = await waitFor(
      () => deadLetterFor(sent.offset),
      'the dead-letter record',
    );
    expect(record.headers['x-dlq-reason']).toContain(
      'max-retries after 3 attempts',
    );
    expect(confirmed.callsFor(eventId)).toBe(3);
  });

  it('loses no message while the dead-letter topic is unavailable', async () => {
    dlqFailures = 3;
    confirmed.behaviour = async () => ({ duplicate: false });
    const poison = await produce('BookingConfirmed', '{"event_id": "broken"');
    const record = await waitFor(
      () => deadLetterFor(poison.offset),
      'the dead-letter record',
    );
    expect(record.value).toBe('{"event_id": "broken"');
    expect(dlqFailures).toBe(0);
    await waitFor(
      async () => (await committedOffset()) === poison.offset + 1,
      'the offset commit',
    );
  });
});
