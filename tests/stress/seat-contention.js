import exec from 'k6/execution';
import { num, text, seconds } from './lib/config.js';
import { createUsers, createEvent, fetchSeats, tokenFor } from './lib/api.js';
import { bookAndAwait, pickSeats, sagaThresholds, SAGA_TIMEOUT_S } from './lib/saga.js';
import { sampleInvariants, verifyFinalState } from './lib/invariants.js';

const userCount = num('STRESS_USERS', 50);
const seatCount = num('STRESS_SEATS', 20);
const seatsPerBooking = num('STRESS_SEATS_PER_BOOKING', 1);
const peakRate = num('STRESS_RATE', 50);
const rampUp = text('STRESS_RAMP_UP', '30s');
const hold = text('STRESS_HOLD', '1m');
const rampDown = '10s';
const totalSeconds = seconds(rampUp) + seconds(hold) + seconds(rampDown);

if (seatsPerBooking < 1 || seatsPerBooking > 20 || seatsPerBooking > seatCount) {
  throw new Error('STRESS_SEATS_PER_BOOKING must be between 1 and min(20, STRESS_SEATS)');
}

export const options = {
  scenarios: {
    attempts: {
      executor: 'ramping-arrival-rate',
      exec: 'attempt',
      startRate: 1,
      timeUnit: '1s',
      stages: [
        { target: peakRate, duration: rampUp },
        { target: peakRate, duration: hold },
        { target: 0, duration: rampDown },
      ],
      preAllocatedVUs: Math.max(20, peakRate * 3),
      maxVUs: num('STRESS_MAX_VUS', Math.max(200, peakRate * 20)),
      gracefulStop: `${SAGA_TIMEOUT_S + 10}s`,
    },
    invariants: {
      executor: 'constant-vus',
      exec: 'sample',
      vus: 1,
      duration: `${totalSeconds}s`,
    },
  },
  thresholds: sagaThresholds(),
  setupTimeout: '10m',
  teardownTimeout: '10m',
};

export function setup() {
  const runId = Date.now().toString(36);
  const users = createUsers(userCount, runId);
  const event = createEvent({ name: `stress-contention-${runId}`, rows: 1, seatsPerRow: seatCount });
  const seats = fetchSeats(event.id);
  return { users, eventId: event.id, seatIds: seats.map((seat) => seat.id) };
}

export function attempt(data) {
  const user = data.users[exec.scenario.iterationInTest % data.users.length];
  bookAndAwait(tokenFor(user), data.eventId, pickSeats(data.seatIds, seatsPerBooking));
}

export function sample() {
  sampleInvariants();
}

export function teardown(data) {
  verifyFinalState(data);
}
