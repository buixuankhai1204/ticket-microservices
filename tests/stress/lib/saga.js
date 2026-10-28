import { check, sleep } from 'k6';
import { num } from './config.js';
import { createBooking, getBooking } from './api.js';
import {
  sagaCompletion,
  bookingsConfirmed,
  bookingsCancelled,
  bookingRejected,
  sagaUnresolved,
} from './metrics.js';

export const SAGA_TIMEOUT_S = num('STRESS_SAGA_TIMEOUT_S', 60);
const POLL_INTERVAL_S = num('STRESS_POLL_INTERVAL_S', 0.25);
const TERMINAL = ['confirmed', 'cancelled'];

export function pickSeats(seatIds, count) {
  const picked = [];
  while (picked.length < count) {
    const id = seatIds[Math.floor(Math.random() * seatIds.length)];
    if (picked.indexOf(id) === -1) {
      picked.push(id);
    }
  }
  return picked;
}

function awaitOutcome(token, bookingId, startedAt) {
  while (Date.now() - startedAt < SAGA_TIMEOUT_S * 1000) {
    const res = getBooking(token, bookingId);
    if (res.status === 200) {
      const booking = res.json();
      if (TERMINAL.indexOf(booking.status) !== -1) {
        return { booking, elapsedMs: Date.now() - startedAt };
      }
    }
    sleep(POLL_INTERVAL_S);
  }
  return null;
}

export function bookAndAwait(token, eventId, seatIds) {
  const startedAt = Date.now();
  const created = createBooking(token, eventId, seatIds);
  const accepted = check(created, { 'booking accepted with 202': (r) => r.status === 202 });
  bookingRejected.add(accepted ? 0 : 1, { status: String(created.status) });
  if (!accepted) {
    return;
  }

  const outcome = awaitOutcome(token, created.json('id'), startedAt);
  sagaUnresolved.add(outcome ? 0 : 1);
  if (!outcome) {
    return;
  }

  const { booking, elapsedMs } = outcome;
  sagaCompletion.add(elapsedMs, { outcome: booking.status });
  bookingsConfirmed.add(booking.status === 'confirmed' ? 1 : 0);
  if (booking.status === 'cancelled') {
    bookingsCancelled.add(1, { reason: booking.failure_reason || 'none' });
  } else {
    bookingsCancelled.add(0);
  }
}

export function sagaThresholds() {
  return {
    http_req_failed: ['rate<0.01'],
    dropped_iterations: ['count==0'],
    'http_req_duration{name:create_booking}': [`p(95)<${num('STRESS_P95_CREATE_MS', 1000)}`],
    saga_completion_time: [`p(95)<${num('STRESS_P95_SAGA_MS', 10000)}`],
    saga_unresolved: ['count==0'],
    booking_rejected: ['count==0'],
    invariant_violations: ['count==0'],
    invariant_scrape_failures: ['count==0'],
  };
}
