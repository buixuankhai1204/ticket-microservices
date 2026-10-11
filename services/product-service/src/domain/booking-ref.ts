import { BookingCancelledError, NotFoundError } from './errors.js';

export type BookingRefStatus = 'confirmed' | 'cancelled';

export interface BookingRefProps {
  bookingId: string;
  userId: string;
  ticketedEventId: string;
  status: BookingRefStatus;
  updatedAt: Date;
}

export class BookingRef {
  readonly bookingId: string;
  readonly userId: string;
  readonly ticketedEventId: string;
  readonly status: BookingRefStatus;
  readonly updatedAt: Date;

  private constructor(props: BookingRefProps) {
    this.bookingId = props.bookingId;
    this.userId = props.userId;
    this.ticketedEventId = props.ticketedEventId;
    this.status = props.status;
    this.updatedAt = props.updatedAt;
  }

  static restore(props: BookingRefProps): BookingRef {
    return new BookingRef(props);
  }

  static confirmed(
    input: Omit<BookingRefProps, 'status' | 'updatedAt'>,
    now: Date,
  ): BookingRef {
    return new BookingRef({ ...input, status: 'confirmed', updatedAt: now });
  }

  static cancelled(
    input: Omit<BookingRefProps, 'status' | 'updatedAt'>,
    now: Date,
  ): BookingRef {
    return new BookingRef({ ...input, status: 'cancelled', updatedAt: now });
  }

  assertOrderableBy(userId: string): void {
    if (this.userId !== userId) {
      throw new NotFoundError('booking not found');
    }
    if (this.status === 'cancelled') {
      throw new BookingCancelledError();
    }
  }
}
