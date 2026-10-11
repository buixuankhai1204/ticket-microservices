import { Injectable } from '@nestjs/common';
import { BookingRef } from '../../../domain/booking-ref.js';
import { BookingRefRepository } from '../../../platform/port/booking-ref.repository.js';
import type { TxContext } from '../../../platform/port/transactor.js';
import { queryRows } from './rows.js';

@Injectable()
export class PostgresBookingRefRepository extends BookingRefRepository {
  async findForShare(
    tx: TxContext,
    bookingId: string,
  ): Promise<BookingRef | null> {
    const rows = await queryRows(
      tx,
      `SELECT booking_id, user_id, ticketed_event_id, status, updated_at
       FROM booking_refs WHERE booking_id = $1 FOR SHARE`,
      [bookingId],
    );
    if (rows.length === 0) {
      return null;
    }
    const row = rows[0];
    return BookingRef.restore({
      bookingId: row.booking_id,
      userId: row.user_id,
      ticketedEventId: row.ticketed_event_id,
      status: row.status,
      updatedAt: row.updated_at,
    });
  }

  async insertConfirmedIfAbsent(tx: TxContext, ref: BookingRef): Promise<void> {
    await queryRows(
      tx,
      `INSERT INTO booking_refs (booking_id, user_id, ticketed_event_id, status, updated_at)
       VALUES ($1, $2, $3, 'confirmed', $4)
       ON CONFLICT (booking_id) DO NOTHING`,
      [ref.bookingId, ref.userId, ref.ticketedEventId, ref.updatedAt],
    );
  }

  async upsertCancelled(tx: TxContext, ref: BookingRef): Promise<void> {
    await queryRows(
      tx,
      `INSERT INTO booking_refs (booking_id, user_id, ticketed_event_id, status, updated_at)
       VALUES ($1, $2, $3, 'cancelled', $4)
       ON CONFLICT (booking_id) DO UPDATE
         SET status = 'cancelled', updated_at = EXCLUDED.updated_at`,
      [ref.bookingId, ref.userId, ref.ticketedEventId, ref.updatedAt],
    );
  }
}
