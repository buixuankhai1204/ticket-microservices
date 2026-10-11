import { Injectable } from '@nestjs/common';
import { BookingNotConfirmedError } from '../domain/errors.js';
import { OrderLimits } from '../domain/order-limits.js';
import { productOrderPlacedEvent } from '../domain/order-events.js';
import {
  ProductOrder,
  priceOrder,
  validateRequestedLines,
  type RequestedLine,
} from '../domain/product-order.js';
import { BookingRefRepository } from '../platform/port/booking-ref.repository.js';
import { EventInventoryRepository } from '../platform/port/event-inventory.repository.js';
import { OutboxRepository } from '../platform/port/outbox.repository.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface PlaceProductOrderInput {
  userId: string;
  bookingId: string;
  lines: RequestedLine[];
}

@Injectable()
export class PlaceProductOrderUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly bookingRefs: BookingRefRepository,
    private readonly inventories: EventInventoryRepository,
    private readonly orders: ProductOrderRepository,
    private readonly outbox: OutboxRepository,
    private readonly limits: OrderLimits,
  ) {}

  async execute(input: PlaceProductOrderInput): Promise<ProductOrder> {
    validateRequestedLines(input.lines, this.limits);
    const now = new Date();
    return this.transactor.run(async (tx) => {
      const ref = await this.bookingRefs.findForShare(tx, input.bookingId);
      if (!ref) {
        throw new BookingNotConfirmedError();
      }
      ref.assertOrderableBy(input.userId);

      const catalog = await this.inventories.priceLines(
        tx,
        ref.ticketedEventId,
        input.lines.map((line) => line.productId),
      );
      const priced = priceOrder(input.lines, catalog, this.limits);
      const order = ProductOrder.place(
        {
          bookingId: input.bookingId,
          userId: input.userId,
          ticketedEventId: ref.ticketedEventId,
          priced,
        },
        now,
      );

      await this.inventories.reserve(
        tx,
        ref.ticketedEventId,
        priced.items,
        now,
      );
      await this.orders.insert(tx, order);
      await this.outbox.write(tx, [productOrderPlacedEvent(order, now)]);
      return order;
    });
  }
}
