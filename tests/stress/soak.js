import exec from 'k6/execution';
import { num, text, seconds } from './lib/config.js';
import {
  createUsers,
  createEvent,
  fetchSeats,
  tokenFor,
  expectOk,
  listEvents,
  getEvent,
  listSeatsPage,
  listProducts,
  listBookingsPage,
} from './lib/api.js';
import { bookAndAwait, sagaThresholds, SAGA_TIMEOUT_S } from './lib/saga.js';
import { sampleInvariants, verifyFinalState } from './lib/invariants.js';

const duration = text('STRESS_DURATION', '30m');
const readRate = num('STRESS_READ_RATE', 30);
const bookingRate = num('STRESS_BOOKING_RATE', 1);
const userCount = num('STRESS_USERS', 30);
const seatCount = num(
  'STRESS_SEATS',
  Math.max(100, Math.ceil(bookingRate * seconds(duration) * 1.2)),
);
const readP95 = num('STRESS_P95_READ_MS', 500);

export const options = {
  scenarios: {
    reads: {
      executor: 'constant-arrival-rate',
      exec: 'reads',
      rate: readRate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: Math.max(20, readRate),
      maxVUs: Math.max(100, readRate * 5),
    },
    bookings: {
      executor: 'constant-arrival-rate',
      exec: 'book',
      rate: bookingRate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: Math.max(10, bookingRate * 5),
      maxVUs: Math.max(50, bookingRate * 30),
      gracefulStop: `${SAGA_TIMEOUT_S + 10}s`,
    },
    invariants: {
      executor: 'constant-vus',
      exec: 'sample',
      vus: 1,
      duration,
    },
  },
  thresholds: Object.assign(sagaThresholds(), {
    checks: ['rate>0.99'],
    bookings_cancelled: ['count==0'],
    'http_req_duration{name:list_events}': [`p(95)<${readP95}`],
    'http_req_duration{name:get_event}': [`p(95)<${readP95}`],
    'http_req_duration{name:list_seats}': [`p(95)<${readP95}`],
    'http_req_duration{name:list_products}': [`p(95)<${readP95}`],
    'http_req_duration{name:list_bookings}': [`p(95)<${readP95}`],
  }),
  setupTimeout: '15m',
  teardownTimeout: '15m',
};

export function setup() {
  const runId = Date.now().toString(36);
  const users = createUsers(userCount, runId);
  const event = createEvent({
    name: `stress-soak-${runId}`,
    rows: Math.ceil(seatCount / 100),
    seatsPerRow: Math.min(100, seatCount),
  });
  const seatIds = fetchSeats(event.id).map((seat) => seat.id);
  const expectedBookings = bookingRate * seconds(duration);
  if (seatIds.length < expectedBookings) {
    console.warn(
      `${seatIds.length} seats for ~${expectedBookings} bookings: seats will be reused and bookings cancelled`,
    );
  }
  return { users, eventId: event.id, seatIds };
}

export function reads(data) {
  const roll = Math.random();
  if (roll < 0.25) {
    expectOk(listEvents(), 'list events');
  } else if (roll < 0.4) {
    expectOk(getEvent(data.eventId), 'get event');
  } else if (roll < 0.65) {
    const pages = Math.ceil(data.seatIds.length / 100);
    expectOk(listSeatsPage(data.eventId, Math.floor(Math.random() * pages) * 100), 'list seats');
  } else if (roll < 0.8) {
    expectOk(listProducts(), 'list products');
  } else {
    const user = data.users[Math.floor(Math.random() * data.users.length)];
    expectOk(listBookingsPage(tokenFor(user), 0), 'list bookings');
  }
}

export function book(data) {
  const n = exec.scenario.iterationInTest;
  const user = data.users[n % data.users.length];
  bookAndAwait(tokenFor(user), data.eventId, [data.seatIds[n % data.seatIds.length]]);
}

export function sample() {
  sampleInvariants();
}

export function teardown(data) {
  verifyFinalState(data);
}
