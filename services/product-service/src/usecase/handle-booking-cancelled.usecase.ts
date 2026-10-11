import { Injectable } from '@nestjs/common';
import { BookingRef } from '../domain/booking-ref.js';
import { BookingRefRepository } from '../platform/port/booking-ref.repository.js';
import { OutboxRepository } from '../platform/port/outbox.repository.js';
import { ProcessedEventRepository } from '../platform/port/processed-event.repository.js';
import { ProductOrderRepository } from '../platform/port/product-order.repository.js';
import { Transactor } from '../platform/port/transactor.js';
import { OrderCanceller } from './order-canceller.js';

export interface BookingCancelledInput {
  eventId: string;
  bookingId: string;
  userId: string;
  ticketedEventId: string;
}

@Injectable()
export class HandleBookingCancelledUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly processed: ProcessedEventRepository,
    private readonly bookingRefs: BookingRefRepository,
    private readonly orders: ProductOrderRepository,
    private readonly outbox: OutboxRepository,
    private readonly canceller: OrderCanceller,
  ) {}

  async execute(
    input: BookingCancelledInput,
  ): Promise<{ duplicate: boolean; cancelledOrders: number }> {
    const now = new Date();
    const ref = BookingRef.cancelled(
      {
        bookingId: input.bookingId,
        userId: input.userId,
        ticketedEventId: input.ticketedEventId,
      },
      now,
    );
    return this.transactor.run(async (tx) => {
      if (!(await this.processed.markProcessed(tx, input.eventId, now))) {
        return { duplicate: true, cancelledOrders: 0 };
      }
      await this.bookingRefs.upsertCancelled(tx, ref);
      const placed = await this.orders.findPlacedByBooking(tx, input.bookingId);
      const { cancelled, events } = await this.canceller.cancel(
        tx,
        placed,
        'booking_cancelled',
        now,
      );
      await this.outbox.write(tx, events);
      return { duplicate: false, cancelledOrders: cancelled.length };
    });
  }
}
