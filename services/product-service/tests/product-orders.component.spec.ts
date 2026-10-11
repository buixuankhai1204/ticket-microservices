import { randomUUID } from 'node:crypto';
import type { NestExpressApplication } from '@nestjs/platform-express';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { HandleBookingCancelledUseCase } from '../src/usecase/handle-booking-cancelled.usecase.js';
import { RecordBookingConfirmedUseCase } from '../src/usecase/record-booking-confirmed.usecase.js';
import { startTestApp, type TestApp } from './common/app.js';
import { USER_ID, signToken } from './common/auth.js';
import * as seed from './common/seed.js';

let ctx: TestApp;
let app: NestExpressApplication;
let recordConfirmed: RecordBookingConfirmedUseCase;
let handleCancelled: HandleBookingCancelledUseCase;

const OTHER_USER_ID = randomUUID();

function uniqueSuffix(): string {
  return randomUUID().replaceAll('-', '').slice(0, 12).toUpperCase();
}

beforeAll(async () => {
  ctx = await startTestApp();
  app = ctx.app;
  recordConfirmed = app.get(RecordBookingConfirmedUseCase);
  handleCancelled = app.get(HandleBookingCancelledUseCase);
});

afterAll(async () => {
  await ctx?.stop();
});

interface Menu {
  eventId: string;
  coffee: string;
  sandwich: string;
}

async function seedMenu(stock = 10): Promise<Menu> {
  const eventId = randomUUID();
  const coffee = await seed.createProduct(app, `COF-${uniqueSuffix()}`, 500);
  const sandwich = await seed.createProduct(app, `SAN-${uniqueSuffix()}`, 300);
  await seed.putInventory(app, eventId, coffee, {
    stock_total: stock,
    price_minor: 450,
  });
  await seed.putInventory(app, eventId, sandwich, { stock_total: stock });
  return { eventId, coffee, sandwich };
}

async function confirmBooking(
  eventId: string,
  userId = USER_ID,
): Promise<string> {
  const bookingId = randomUUID();
  await recordConfirmed.execute({
    eventId: randomUUID(),
    bookingId,
    userId,
    ticketedEventId: eventId,
  });
  return bookingId;
}

async function placeOrder(
  bookingId: string,
  items: Array<{ product_id: string; quantity: number }>,
  userId = USER_ID,
) {
  return request(app.getHttpServer())
    .post('/api/v1/product-orders')
    .set('Authorization', `Bearer ${await signToken(userId)}`)
    .send({ booking_id: bookingId, items });
}

async function cancelOrder(orderId: string, userId = USER_ID) {
  return request(app.getHttpServer())
    .delete(`/api/v1/product-orders/${orderId}`)
    .set('Authorization', `Bearer ${await signToken(userId)}`);
}

async function stock(menu: Menu, productId: string): Promise<number> {
  const response = await request(app.getHttpServer())
    .get(`/api/v1/products/events/${menu.eventId}/menu`)
    .expect(200);
  const line = response.body.data.find(
    (entry: { product: { id: string } }) => entry.product.id === productId,
  );
  return line.stock_available;
}

async function publishedFor(bookingId: string) {
  const rows = await ctx.db.query(
    `SELECT event_type, payload FROM outbox_tap WHERE aggregate_id = $1 ORDER BY created_at, id`,
    [bookingId],
  );
  return rows.rows as Array<{
    event_type: string;
    payload: Record<string, any>;
  }>;
}

describe('placing an order', () => {
  it('prices from the event menu, takes the stock and publishes ProductOrderPlaced', async () => {
    const menu = await seedMenu();
    const bookingId = await confirmBooking(menu.eventId);

    const response = await placeOrder(bookingId, [
      { product_id: menu.coffee, quantity: 2 },
      { product_id: menu.sandwich, quantity: 1 },
    ]).then((r) => {
      expect(r.status).toBe(201);
      return r;
    });

    expect(response.body).toMatchObject({
      booking_id: bookingId,
      ticketed_event_id: menu.eventId,
      status: 'placed',
      currency: 'USD',
      total_minor: 2 * 450 + 300,
      item_count: 3,
    });
    expect(response.body.items).toHaveLength(2);
    expect(await stock(menu, menu.coffee)).toBe(8);
    expect(await stock(menu, menu.sandwich)).toBe(9);

    const events = await publishedFor(bookingId);
    expect(events.map((e) => e.event_type)).toEqual(['ProductOrderPlaced']);
    expect(events[0].payload).toMatchObject({
      order_id: response.body.id,
      booking_id: bookingId,
      user_id: USER_ID,
      ticketed_event_id: menu.eventId,
      total_minor: 1200,
      item_count: 3,
    });
    expect(events[0].payload.items).toHaveLength(2);

    const fetched = await request(app.getHttpServer())
      .get(`/api/v1/product-orders/${response.body.id}`)
      .set('Authorization', `Bearer ${await signToken(USER_ID)}`)
      .expect(200);
    expect(fetched.body.id).toBe(response.body.id);
    await request(app.getHttpServer())
      .get(`/api/v1/product-orders/${response.body.id}`)
      .set('Authorization', `Bearer ${await signToken(OTHER_USER_ID)}`)
      .expect(404);

    const list = await request(app.getHttpServer())
      .get('/api/v1/product-orders?limit=100')
      .set('Authorization', `Bearer ${await signToken(USER_ID)}`)
      .expect(200);
    expect(
      list.body.data.some((o: { id: string }) => o.id === response.body.id),
    ).toBe(true);
    expect(list.body.pagination.limit).toBe(100);
  });

  it('rejects what it cannot place without touching stock or publishing', async () => {
    const menu = await seedMenu();
    const item = (productId: string, quantity = 1) => ({
      product_id: productId,
      quantity,
    });

    const unconfirmed = await placeOrder(randomUUID(), [item(menu.coffee)]);
    expect(unconfirmed.status).toBe(409);
    expect(unconfirmed.body).toEqual({ error: 'booking not confirmed' });

    const foreign = await confirmBooking(menu.eventId, OTHER_USER_ID);
    expect((await placeOrder(foreign, [item(menu.coffee)])).status).toBe(404);

    const bookingId = await confirmBooking(menu.eventId);
    expect((await placeOrder(bookingId, [item(menu.coffee, 0)])).status).toBe(
      400,
    );
    expect(
      (await placeOrder(bookingId, [item(menu.coffee), item(menu.coffee)]))
        .status,
    ).toBe(400);

    const notOnMenu = await placeOrder(bookingId, [item(randomUUID())]);
    expect(notOnMenu.status).toBe(409);
    expect(notOnMenu.body.error).toContain('product not available');

    const euro = await seed.createProduct(
      app,
      `EUR-${uniqueSuffix()}`,
      100,
      'EUR',
    );
    await seed.putInventory(app, menu.eventId, euro, { stock_total: 5 });
    expect(
      (await placeOrder(bookingId, [item(menu.coffee), item(euro)])).status,
    ).toBe(400);

    const tooMany = await placeOrder(bookingId, [item(menu.coffee, 11)]);
    expect(tooMany.status).toBe(400);

    await seed.adjustStock(app, menu.eventId, menu.sandwich, -9);
    const short = await placeOrder(bookingId, [
      item(menu.coffee, 2),
      item(menu.sandwich, 2),
    ]);
    expect(short.status).toBe(409);
    expect(short.body).toEqual({ error: 'insufficient stock' });
    expect(await stock(menu, menu.coffee)).toBe(10);
    expect(await stock(menu, menu.sandwich)).toBe(1);
    expect(await publishedFor(bookingId)).toEqual([]);

    expect((await placeOrder(bookingId, [item(menu.coffee)])).status).toBe(201);
    const second = await placeOrder(bookingId, [item(menu.coffee)]);
    expect(second.status).toBe(409);
    expect(second.body).toEqual({ error: 'order already exists for booking' });
  });

  it('never oversells under concurrent orders', async () => {
    const menu = await seedMenu(3);
    const bookings = await Promise.all(
      Array.from({ length: 10 }, () => confirmBooking(menu.eventId)),
    );
    const results = await Promise.all(
      bookings.map((bookingId) =>
        placeOrder(bookingId, [{ product_id: menu.coffee, quantity: 1 }]),
      ),
    );
    const statuses = results.map((r) => r.status);
    expect(statuses.filter((s) => s === 201)).toHaveLength(3);
    expect(statuses.filter((s) => s === 409)).toHaveLength(7);
    expect(await stock(menu, menu.coffee)).toBe(0);
  });
});

describe('cancelling an order', () => {
  it('restocks and publishes once, is owner-only and idempotent, and allows ordering again', async () => {
    const menu = await seedMenu();
    const bookingId = await confirmBooking(menu.eventId);
    const placed = await placeOrder(bookingId, [
      { product_id: menu.coffee, quantity: 3 },
    ]);
    expect(placed.status).toBe(201);
    expect(await stock(menu, menu.coffee)).toBe(7);

    expect((await cancelOrder(placed.body.id, OTHER_USER_ID)).status).toBe(404);

    const cancelled = await cancelOrder(placed.body.id);
    expect(cancelled.status).toBe(200);
    expect(cancelled.body).toMatchObject({
      status: 'cancelled',
      cancel_reason: 'customer_cancelled',
    });
    expect(await stock(menu, menu.coffee)).toBe(10);

    const again = await cancelOrder(placed.body.id);
    expect(again.status).toBe(200);
    expect(await stock(menu, menu.coffee)).toBe(10);

    const events = await publishedFor(bookingId);
    expect(events.map((e) => e.event_type)).toEqual([
      'ProductOrderPlaced',
      'ProductOrderCancelled',
    ]);
    expect(events[1].payload).toMatchObject({
      order_id: placed.body.id,
      reason: 'customer_cancelled',
      total_minor: 3 * 450,
    });

    const replaced = await placeOrder(bookingId, [
      { product_id: menu.coffee, quantity: 1 },
    ]);
    expect(replaced.status).toBe(201);
  });
});

describe('booking events', () => {
  it('cancels placed orders when the booking is cancelled, once, and never resurrects it', async () => {
    const menu = await seedMenu();
    const bookingId = await confirmBooking(menu.eventId);
    const placed = await placeOrder(bookingId, [
      { product_id: menu.coffee, quantity: 4 },
    ]);
    expect(placed.status).toBe(201);
    expect(await stock(menu, menu.coffee)).toBe(6);

    const cancelEvent = {
      eventId: randomUUID(),
      bookingId,
      userId: USER_ID,
      ticketedEventId: menu.eventId,
    };
    expect(await handleCancelled.execute(cancelEvent)).toEqual({
      duplicate: false,
      cancelledOrders: 1,
    });
    expect(await stock(menu, menu.coffee)).toBe(10);
    expect(await handleCancelled.execute(cancelEvent)).toEqual({
      duplicate: true,
      cancelledOrders: 0,
    });
    expect(await stock(menu, menu.coffee)).toBe(10);

    const events = await publishedFor(bookingId);
    expect(events.map((e) => e.event_type)).toEqual([
      'ProductOrderPlaced',
      'ProductOrderCancelled',
    ]);
    expect(events[1].payload.reason).toBe('booking_cancelled');

    const fetched = await request(app.getHttpServer())
      .get(`/api/v1/product-orders/${placed.body.id}`)
      .set('Authorization', `Bearer ${await signToken(USER_ID)}`)
      .expect(200);
    expect(fetched.body.status).toBe('cancelled');

    await recordConfirmed.execute({
      eventId: randomUUID(),
      bookingId,
      userId: USER_ID,
      ticketedEventId: menu.eventId,
    });
    const blocked = await placeOrder(bookingId, [
      { product_id: menu.coffee, quantity: 1 },
    ]);
    expect(blocked.status).toBe(409);
    expect(blocked.body).toEqual({ error: 'booking cancelled' });
  });
});
