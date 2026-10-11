import type { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import type { Client } from 'pg';
import { ADMIN_ID, JWT_ISSUER, JWT_SECRET } from './auth.js';
import { createTestDatabase, type TestDatabase } from './postgres.js';

export interface TestApp {
  app: NestExpressApplication;
  db: Client;
  stop(): Promise<void>;
}

const OUTBOX_TAP_SQL = `
  CREATE TABLE outbox_tap (LIKE outbox_events);
  CREATE FUNCTION tap_outbox() RETURNS trigger AS $$
  BEGIN
    INSERT INTO outbox_tap SELECT NEW.*;
    RETURN NEW;
  END
  $$ LANGUAGE plpgsql;
  CREATE TRIGGER outbox_tap_insert AFTER INSERT ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION tap_outbox();
`;

export async function startTestApp(): Promise<TestApp> {
  const database: TestDatabase = await createTestDatabase();
  process.env.DATABASE_URL = database.url;
  process.env.JWT_SECRET = JWT_SECRET;
  process.env.JWT_ISSUER = JWT_ISSUER;
  process.env.PRODUCT_ADMIN_USER_IDS = ADMIN_ID;
  process.env.LOG_LEVEL = 'silent';
  process.env.KAFKA_CONSUMER_ENABLED = 'false';

  const { AppModule } = await import('../../src/app.module.js');
  const { configureApp } = await import('../../src/app.setup.js');
  const moduleRef = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>();
  configureApp(app);
  await app.init();

  const db = await database.connect();
  await db.query(OUTBOX_TAP_SQL);
  return {
    app,
    db,
    async stop() {
      await app.close();
      await db.end();
      await database.drop();
    },
  };
}
