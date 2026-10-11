import { z } from 'zod';
import type { BookingCancelledInput } from '../../../usecase/handle-booking-cancelled.usecase.js';
import type { BookingConfirmedInput } from '../../../usecase/record-booking-confirmed.usecase.js';
import type { ConsumedMessage } from './consumed-message.js';

export class PoisonMessageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PoisonMessageError';
  }
}

const bookingEventSchema = z.object({
  event_id: z.uuid(),
  booking_id: z.uuid(),
  user_id: z.uuid(),
  ticketed_event_id: z.uuid(),
});

export type BookingEvent =
  | { kind: 'confirmed'; input: BookingConfirmedInput }
  | { kind: 'cancelled'; input: BookingCancelledInput }
  | { kind: 'skip'; eventType: string };

function parseJson(message: ConsumedMessage): unknown {
  if (message.value === null || message.value.length === 0) {
    throw new PoisonMessageError('empty payload');
  }
  try {
    return JSON.parse(message.value.toString());
  } catch {
    throw new PoisonMessageError('payload is not valid JSON');
  }
}

export function parseBookingEvent(message: ConsumedMessage): BookingEvent {
  const eventType = message.headers.event_type;
  if (!eventType) {
    throw new PoisonMessageError('missing event_type header');
  }
  if (eventType !== 'BookingConfirmed' && eventType !== 'BookingCancelled') {
    return { kind: 'skip', eventType };
  }
  const parsed = bookingEventSchema.safeParse(parseJson(message));
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `${issue.path.join('.') || 'payload'}: ${issue.message}`)
      .join('; ');
    throw new PoisonMessageError(`invalid ${eventType} payload: ${issues}`);
  }
  const input = {
    eventId: parsed.data.event_id,
    bookingId: parsed.data.booking_id,
    userId: parsed.data.user_id,
    ticketedEventId: parsed.data.ticketed_event_id,
  };
  return eventType === 'BookingConfirmed'
    ? { kind: 'confirmed', input }
    : { kind: 'cancelled', input };
}
