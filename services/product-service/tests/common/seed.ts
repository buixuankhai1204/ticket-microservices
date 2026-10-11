import type { NestExpressApplication } from '@nestjs/platform-express';
import request from 'supertest';
import { ADMIN_ID, signToken } from './auth.js';

export async function createProduct(
  app: NestExpressApplication,
  sku: string,
  priceMinor = 500,
  currency?: string,
): Promise<string> {
  const response = await request(app.getHttpServer())
    .post('/api/v1/products')
    .set('Authorization', `Bearer ${await signToken(ADMIN_ID)}`)
    .send({
      sku,
      name: `Product ${sku}`,
      category: 'snack',
      price_minor: priceMinor,
      ...(currency ? { currency } : {}),
    })
    .expect(201);
  return response.body.id;
}

export async function putInventory(
  app: NestExpressApplication,
  eventId: string,
  productId: string,
  body: Record<string, unknown>,
) {
  return request(app.getHttpServer())
    .put(`/api/v1/products/events/${eventId}/inventory/${productId}`)
    .set('Authorization', `Bearer ${await signToken(ADMIN_ID)}`)
    .send(body);
}

export async function adjustStock(
  app: NestExpressApplication,
  eventId: string,
  productId: string,
  delta: number,
) {
  return request(app.getHttpServer())
    .post(
      `/api/v1/products/events/${eventId}/inventory/${productId}/stock-adjustments`,
    )
    .set('Authorization', `Bearer ${await signToken(ADMIN_ID)}`)
    .send({ delta, reason: 'component test' });
}
