import { randomUUID } from 'node:crypto';
import type { OutboxEvent } from './outbox-event.js';
import type { CancelReason, ProductOrder } from './product-order.js';

export const PRODUCT_ORDER_AGGREGATE = 'product_order';

function identity(order: ProductOrder, eventId: string) {
  return {
    event_id: eventId,
    order_id: order.id,
    booking_id: order.bookingId,
    user_id: order.userId,
    ticketed_event_id: order.ticketedEventId,
    currency: order.currency,
    total_minor: order.totalMinor,
    item_count: order.itemCount,
  };
}

export function productOrderPlacedEvent(
  order: ProductOrder,
  now: Date,
): OutboxEvent {
  const id = randomUUID();
  return {
    id,
    aggregateId: order.bookingId,
    aggregateType: PRODUCT_ORDER_AGGREGATE,
    eventType: 'ProductOrderPlaced',
    payload: {
      ...identity(order, id),
      items: order.items.map((item) => ({
        product_id: item.productId,
        name: item.name,
        quantity: item.quantity,
        unit_price_minor: item.unitPriceMinor,
        line_total_minor: item.lineTotalMinor,
      })),
      placed_at: order.createdAt.toISOString(),
    },
    createdAt: now,
  };
}

export function productOrderCancelledEvent(
  order: ProductOrder,
  reason: CancelReason,
  now: Date,
): OutboxEvent {
  const id = randomUUID();
  return {
    id,
    aggregateId: order.bookingId,
    aggregateType: PRODUCT_ORDER_AGGREGATE,
    eventType: 'ProductOrderCancelled',
    payload: {
      ...identity(order, id),
      reason,
      cancelled_at: now.toISOString(),
    },
    createdAt: now,
  };
}
