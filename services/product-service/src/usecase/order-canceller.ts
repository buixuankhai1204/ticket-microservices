import { Injectable } from '@nestjs/common';
import { productOrderCancelledEvent } from '../domain/order-events.js';
import type { OutboxEvent } from '../domain/outbox-event.js';
import type {
  CancelReason,
  ProductOrder,
  RequestedLine,
} from '../domain/product-order.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import type { TxContext } from '../platform/port/transactor.js';

export interface CancellationResult {
  cancelled: ProductOrder[];
  events: OutboxEvent[];
}

@Injectable()
export class OrderCanceller {
  constructor(
    private readonly orders: ProductOrderRepository,
    private readonly inventories: EventInventoryRepository,
  ) {}

  async cancel(
    tx: TxContext,
    placed: ProductOrder[],
    reason: CancelReason,
    now: Date,
  ): Promise<CancellationResult> {
    if (placed.length === 0) {
      return { cancelled: [], events: [] };
    }
    await this.orders.markCancelled(
      tx,
      placed.map((order) => order.id),
      reason,
      now,
    );
    const byEvent = new Map<string, Map<string, number>>();
    for (const order of placed) {
      const quantities = byEvent.get(order.ticketedEventId) ?? new Map();
      for (const item of order.items) {
        quantities.set(
          item.productId,
          (quantities.get(item.productId) ?? 0) + item.quantity,
        );
      }
      byEvent.set(order.ticketedEventId, quantities);
    }
    for (const [ticketedEventId, quantities] of byEvent) {
      const lines: RequestedLine[] = [...quantities].map(
        ([productId, quantity]) => ({ productId, quantity }),
      );
      await this.inventories.restock(tx, ticketedEventId, lines, now);
    }
    const cancelled = placed.map((order) => order.cancel(reason, now));
    return {
      cancelled,
      events: cancelled.map((order) =>
        productOrderCancelledEvent(order, reason, now),
      ),
    };
  }
}
