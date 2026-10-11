import { Injectable } from '@nestjs/common';
import { BookingRef } from '../domain/booking-ref.js';
import { BookingRefRepository } from '../platform/port/booking-ref.repository.js';
import { ProcessedEventRepository } from '../platform/port/processed-event.repository.js';
import { Transactor } from '../platform/port/transactor.js';

export interface BookingConfirmedInput {
  eventId: string;
  bookingId: string;
  userId: string;
  ticketedEventId: string;
}

@Injectable()
export class RecordBookingConfirmedUseCase {
  constructor(
    private readonly transactor: Transactor,
    private readonly processed: ProcessedEventRepository,
    private readonly bookingRefs: BookingRefRepository,
  ) {}

  async execute(input: BookingConfirmedInput): Promise<{ duplicate: boolean }> {
    const now = new Date();
    const ref = BookingRef.confirmed(
      {
        bookingId: input.bookingId,
        userId: input.userId,
        ticketedEventId: input.ticketedEventId,
      },
      now,
    );
    return this.transactor.run(async (tx) => {
      if (!(await this.processed.markProcessed(tx, input.eventId, now))) {
        return { duplicate: true };
      }
      await this.bookingRefs.insertConfirmedIfAbsent(tx, ref);
      return { duplicate: false };
    });
  }
}
