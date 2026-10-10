import { randomUUID } from 'node:crypto';
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import { SignJWT } from 'jose';
import { Client } from 'pg';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const JWT_SECRET = 'component-test-secret-0123456789';
const ADMIN_ID = randomUUID();
const USER_ID = randomUUID();
const dbName = `product_test_${randomUUID().replaceAll('-', '')}`;

let app: NestExpressApplication;
let admin: Client;
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

async function token(sub: string): Promise<string> {
  return new SignJWT({})
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuer('user-service')
    .setSubject(sub)
    .setExpirationTime('1h')
    .sign(new TextEncoder().encode(JWT_SECRET));
}

async function createProduct(sku: string, priceMinor = 500): Promise<string> {
  const response = await request(app.getHttpServer())
    .post('/api/v1/products')
    .set('Authorization', `Bearer ${await token(ADMIN_ID)}`)
    .send({
      sku,
      name: `Product ${sku}`,
      category: 'snack',
      price_minor: priceMinor,
    })
    .expect(201);
  return response.body.id;
}

async function putInventory(
  eventId: string,
  productId: string,
  body: Record<string, unknown>,
) {
  return request(app.getHttpServer())
    .put(`/api/v1/products/events/${eventId}/inventory/${productId}`)
    .set('Authorization', `Bearer ${await token(ADMIN_ID)}`)
    .send(body);
}

async function adjust(eventId: string, productId: string, delta: number) {
  return request(app.getHttpServer())
    .post(
      `/api/v1/products/events/${eventId}/inventory/${productId}/stock-adjustments`,
    )
    .set('Authorization', `Bearer ${await token(ADMIN_ID)}`)
    .send({ delta, reason: 'component test' });
}

beforeAll(async () => {
  const adminConnection = await adminUrl();
  admin = new Client({ connectionString: adminConnection });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${dbName}"`);

  const url = new URL(adminConnection);
  url.pathname = `/${dbName}`;
  process.env.DATABASE_URL = url.toString();
  process.env.JWT_SECRET = JWT_SECRET;
  process.env.JWT_ISSUER = 'user-service';
  process.env.PRODUCT_ADMIN_USER_IDS = ADMIN_ID;
  process.env.LOG_LEVEL = 'silent';

  const { AppModule } = await import('../src/app.module.js');
  const { configureApp } = await import('../src/app.setup.js');
  const moduleRef = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();
  app = moduleRef.createNestApplication<NestExpressApplication>();
  configureApp(app);
  await app.init();
});

afterAll(async () => {
  await app?.close();
  await admin?.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
  await admin?.end();
  await container?.stop();
});

describe('health', () => {
  it('reports live and ready, and exposes request metrics', async () => {
    await request(app.getHttpServer()).get('/healthz').expect(200);
    await request(app.getHttpServer()).get('/readyz').expect(200);
    await request(app.getHttpServer()).get('/api/v1/products').expect(200);
    const metrics = await request(app.getHttpServer())
      .get('/metrics')
      .expect(200);
    expect(metrics.text).toContain('http_requests_total');
    expect(metrics.text).toContain('db_pool_connections_max');
  });
});

describe('catalog', () => {
  it('rejects unauthenticated and non-admin writes with the error envelope', async () => {
    const body = { sku: 'NOPE', name: 'x', category: 'food', price_minor: 1 };
    const anonymous = await request(app.getHttpServer())
      .post('/api/v1/products')
      .send(body)
      .expect(401);
    expect(anonymous.body).toEqual({
      error: 'invalid or missing bearer token',
    });
    await request(app.getHttpServer())
      .post('/api/v1/products')
      .set('Authorization', `Bearer ${await token(USER_ID)}`)
      .send(body)
      .expect(403);
  });

  it('creates, reads, updates and rejects a duplicate sku', async () => {
    const id = await createProduct('CAT-1', 450);
    const read = await request(app.getHttpServer())
      .get(`/api/v1/products/${id}`)
      .expect(200);
    expect(read.body).toMatchObject({
      sku: 'CAT-1',
      price_minor: 450,
      currency: 'USD',
    });

    const updated = await request(app.getHttpServer())
      .put(`/api/v1/products/${id}`)
      .set('Authorization', `Bearer ${await token(ADMIN_ID)}`)
      .send({ price_minor: 500 })
      .expect(200);
    expect(updated.body.price_minor).toBe(500);

    const duplicate = await request(app.getHttpServer())
      .post('/api/v1/products')
      .set('Authorization', `Bearer ${await token(ADMIN_ID)}`)
      .send({ sku: 'CAT-1', name: 'again', category: 'snack', price_minor: 1 })
      .expect(409);
    expect(duplicate.body).toEqual({ error: 'sku already exists' });
  });

  it('paginates with the envelope, clamps the limit and rejects bad input', async () => {
    for (const n of [1, 2, 3]) {
      await createProduct(`PAGE-${n}`);
    }
    const page = await request(app.getHttpServer())
      .get('/api/v1/products?limit=2&offset=0')
      .expect(200);
    expect(page.body.data).toHaveLength(2);
    expect(page.body.pagination).toMatchObject({
      limit: 2,
      offset: 0,
      has_more: true,
    });
    expect(page.body.pagination.total).toBeGreaterThanOrEqual(4);

    const clamped = await request(app.getHttpServer())
      .get('/api/v1/products?limit=1000')
      .expect(200);
    expect(clamped.body.pagination.limit).toBe(100);

    await request(app.getHttpServer())
      .get('/api/v1/products?limit=-1')
      .expect(400);
    await request(app.getHttpServer())
      .get('/api/v1/products?category[$ne]=food')
      .expect(400);
    await request(app.getHttpServer())
      .get('/api/v1/products/not-a-uuid')
      .expect(400);
    const missing = await request(app.getHttpServer())
      .get(`/api/v1/products/${randomUUID()}`)
      .expect(404);
    expect(missing.body).toEqual({ error: 'product not found' });
  });
});

describe('inventory', () => {
  it('serves a per-event menu with the event price override', async () => {
    const eventId = randomUUID();
    const productId = await createProduct('MENU-1', 700);
    await putInventory(eventId, productId, {
      stock_total: 10,
      price_minor: 600,
    }).then((response) => expect(response.status).toBe(200));
    const menu = await request(app.getHttpServer())
      .get(`/api/v1/products/events/${eventId}/menu`)
      .expect(200);
    expect(menu.body.data).toHaveLength(1);
    expect(menu.body.data[0]).toMatchObject({
      unit_price_minor: 600,
      stock_available: 10,
      in_stock: true,
    });

    const empty = await request(app.getHttpServer())
      .get(`/api/v1/products/events/${randomUUID()}/menu`)
      .expect(200);
    expect(empty.body.data).toEqual([]);
  });

  it('never lets stock go negative under concurrent decrements', async () => {
    const eventId = randomUUID();
    const productId = await createProduct('RACE-1');
    expect(
      (await putInventory(eventId, productId, { stock_total: 5 })).status,
    ).toBe(200);

    const results = await Promise.all(
      Array.from({ length: 20 }, () => adjust(eventId, productId, -1)),
    );
    const statuses = results.map((response) => response.status);
    expect(statuses.filter((status) => status === 200)).toHaveLength(5);
    expect(statuses.filter((status) => status === 409)).toHaveLength(15);

    const menu = await request(app.getHttpServer())
      .get(`/api/v1/products/events/${eventId}/menu`)
      .expect(200);
    expect(menu.body.data[0]).toMatchObject({
      stock_available: 0,
      in_stock: false,
    });
  });

  it('answers 404 for stock of an unknown inventory and for an unknown product', async () => {
    const eventId = randomUUID();
    const productId = await createProduct('MISSING-1');
    expect((await adjust(eventId, productId, 1)).status).toBe(404);
    expect(
      (await putInventory(eventId, randomUUID(), { stock_total: 1 })).status,
    ).toBe(404);
  });
});
