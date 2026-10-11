import { randomUUID } from 'node:crypto';
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import { Client } from 'pg';

let container: StartedPostgreSqlContainer | undefined;

async function adminUrl(): Promise<string> {
  const configured = process.env.TEST_DATABASE_URL;
  if (configured) {
    return configured;
  }
  container = await new PostgreSqlContainer('postgres:16-alpine')
    .withDatabase('postgres')
    .withUsername('postgres')
    .withPassword('postgres')
    .start();
  return container.getConnectionUri();
}

export interface TestDatabase {
  url: string;
  connect(): Promise<Client>;
  drop(): Promise<void>;
}

export async function createTestDatabase(): Promise<TestDatabase> {
  const adminConnection = await adminUrl();
  const admin = new Client({ connectionString: adminConnection });
  await admin.connect();
  const name = `product_test_${randomUUID().replaceAll('-', '')}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(adminConnection);
  url.pathname = `/${name}`;
  return {
    url: url.toString(),
    async connect() {
      const client = new Client({ connectionString: url.toString() });
      await client.connect();
      return client;
    },
    async drop() {
      await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
      await admin.end();
      await container?.stop();
    },
  };
}
