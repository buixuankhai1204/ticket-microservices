import { Test } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ADMIN_ID, JWT_ISSUER, JWT_SECRET } from './common/auth.js';
import { createTestDatabase, type TestDatabase } from './common/postgres.js';

let database: TestDatabase;

beforeAll(async () => {
  database = await createTestDatabase();
  process.env.DATABASE_URL = database.url;
  process.env.JWT_SECRET = JWT_SECRET;
  process.env.JWT_ISSUER = JWT_ISSUER;
  process.env.PRODUCT_ADMIN_USER_IDS = ADMIN_ID;
  process.env.LOG_LEVEL = 'silent';
  process.env.KAFKA_CONSUMER_ENABLED = 'true';
  process.env.KAFKA_BROKERS = 'localhost:9092';
});

afterAll(async () => {
  await database?.drop();
});

describe('messaging wiring', () => {
  it('resolves the booking events consumer and its use cases from the real module graph', async () => {
    const { AppModule } = await import('../src/app.module.js');
    const { DeadLetterPublisher } =
      await import('../src/adapter/messaging/kafka/dead-letter.publisher.js');
    const { BookingEventsController } =
      await import('../src/adapter/messaging/kafka/booking-events.controller.js');
    const { BookingEventsProcessor } =
      await import('../src/adapter/messaging/kafka/booking-events.processor.js');

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(DeadLetterPublisher)
      .useValue({ publish: async () => undefined })
      .compile();

    expect(
      moduleRef.get(BookingEventsController, { strict: false }),
    ).toBeDefined();
    expect(
      moduleRef.get(BookingEventsProcessor, { strict: false }),
    ).toBeDefined();
    await moduleRef.close();
  });
});
