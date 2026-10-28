import http from 'k6/http';
import { sleep } from 'k6';
import { urls, num } from './config.js';
import { fetchSeats, listMyBookings, tokenFor } from './api.js';
import {
  invariantViolations,
  invariantScrapeFailures,
  sampledStuckPending,
  sampledDbPoolInUse,
} from './metrics.js';

const SAMPLE_INTERVAL_S = num('STRESS_SAMPLE_INTERVAL_S', 10);
const DRAIN_TIMEOUT_S = num('STRESS_DRAIN_TIMEOUT_S', 240);
const DRAIN_POLL_S = 3;
const TERMINAL = ['confirmed', 'cancelled'];
const GAUGES = ['bookings_oversold_seats', 'bookings_stuck_pending', 'db_pool_connections_in_use'];

function scrape(names) {
  const res = http.get(`${urls.bookingMetrics}/metrics`, { tags: { name: 'metrics' } });
  if (res.status !== 200) {
    invariantScrapeFailures.add(1);
    return null;
  }
  const values = {};
  names.forEach((name) => {
    const match = new RegExp(`^${name}(?:\\{[^}]*\\})?\\s+(\\S+)`, 'm').exec(res.body);
    values[name] = match ? Number(match[1]) : null;
  });
  invariantScrapeFailures.add(names.some((name) => values[name] === null) ? 1 : 0);
  return values;
}

function gauge(values, name) {
  return values && values[name] !== null ? values[name] : 0;
}

export function sampleInvariants() {
  const values = scrape(GAUGES);
  if (values) {
    const oversold = gauge(values, 'bookings_oversold_seats');
    if (oversold > 0) {
      invariantViolations.add(oversold, { kind: 'oversold_seats' });
      console.error(`booking-service reports ${oversold} oversold seats`);
    }
    sampledStuckPending.add(gauge(values, 'bookings_stuck_pending'));
    sampledDbPoolInUse.add(gauge(values, 'db_pool_connections_in_use'));
  }
  sleep(SAMPLE_INTERVAL_S);
}

function snapshot(data) {
  let bookings = [];
  data.users.forEach((user) => {
    bookings = bookings.concat(listMyBookings(tokenFor(user)));
  });
  return {
    seats: fetchSeats(data.eventId),
    bookings: bookings.filter((booking) => booking.event_id === data.eventId),
    gauges: scrape(GAUGES),
  };
}

function heldSeats(state) {
  return state.seats.filter((seat) => seat.status === 'reserved').length;
}

function pendingBookings(state) {
  return state.bookings.filter((booking) => TERMINAL.indexOf(booking.status) === -1).length;
}

function drained(state) {
  return (
    heldSeats(state) === 0 &&
    pendingBookings(state) === 0 &&
    gauge(state.gauges, 'bookings_stuck_pending') === 0
  );
}

export function verifyFinalState(data) {
  invariantViolations.add(0);

  const deadline = Date.now() + DRAIN_TIMEOUT_S * 1000;
  let state = snapshot(data);
  while (!drained(state) && Date.now() < deadline) {
    sleep(DRAIN_POLL_S);
    state = snapshot(data);
  }

  const confirmedBy = {};
  state.bookings
    .filter((booking) => booking.status === 'confirmed')
    .forEach((booking) => {
      booking.seat_ids.forEach((id) => {
        confirmedBy[id] = (confirmedBy[id] || 0) + 1;
      });
    });
  const confirmedSeats = Object.keys(confirmedBy);
  const booked = state.seats.filter((seat) => seat.status === 'booked').length;

  const findings = {
    double_confirmed_seat: confirmedSeats.filter((id) => confirmedBy[id] > 1).length,
    seat_state_mismatch: Math.abs(booked - confirmedSeats.length),
    held_seats_not_released: heldSeats(state),
    pending_booking: pendingBookings(state),
    stuck_pending: gauge(state.gauges, 'bookings_stuck_pending'),
    oversold_seats: gauge(state.gauges, 'bookings_oversold_seats'),
  };
  Object.keys(findings).forEach((kind) => {
    if (findings[kind] > 0) {
      invariantViolations.add(findings[kind], { kind });
      console.error(`invariant violated: ${kind} = ${findings[kind]}`);
    }
  });

  console.log(
    `final state: ${state.seats.length} seats, ${booked} booked, ` +
      `${confirmedSeats.length} seats confirmed across ${state.bookings.length} bookings`,
  );
}
