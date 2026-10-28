import { num, text } from './lib/config.js';
import {
  createUsers,
  createEvent,
  tokenFor,
  expectOk,
  listEvents,
  getEvent,
  listSeatsPage,
  listProducts,
  listBookingsPage,
} from './lib/api.js';

const rate = num('STRESS_RATE', 20);
const p95 = num('STRESS_P95_MS', 500);

export const options = {
  scenarios: {
    reads: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration: text('STRESS_DURATION', '1m'),
      preAllocatedVUs: Math.max(10, rate),
      maxVUs: Math.max(50, rate * 5),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
    dropped_iterations: ['count==0'],
    'http_req_duration{name:list_events}': [`p(95)<${p95}`],
    'http_req_duration{name:get_event}': [`p(95)<${p95}`],
    'http_req_duration{name:list_seats}': [`p(95)<${p95}`],
    'http_req_duration{name:list_products}': [`p(95)<${p95}`],
    'http_req_duration{name:list_bookings}': [`p(95)<${p95}`],
  },
  setupTimeout: '2m',
};

export function setup() {
  const runId = Date.now().toString(36);
  const [user] = createUsers(1, runId);
  const event = createEvent({ name: `stress-smoke-${runId}`, rows: 2, seatsPerRow: 10 });
  return { user, eventId: event.id };
}

export default function (data) {
  expectOk(listEvents(), 'list events');
  expectOk(getEvent(data.eventId), 'get event');
  expectOk(listSeatsPage(data.eventId, 0), 'list seats');
  expectOk(listProducts(), 'list products');
  expectOk(listBookingsPage(tokenFor(data.user), 0), 'list bookings');
}
