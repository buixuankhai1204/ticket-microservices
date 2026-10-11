import type { BookingRef } from '../../domain/booking-ref.js';
import type { TxContext } from './transactor.js';

export abstract class BookingRefRepository {
  abstract findForShare(
    tx: TxContext,
    bookingId: string,
  ): Promise<BookingRef | null>;
  abstract insertConfirmedIfAbsent(
    tx: TxContext,
    ref: BookingRef,
  ): Promise<void>;
  abstract upsertCancelled(tx: TxContext, ref: BookingRef): Promise<void>;
}
