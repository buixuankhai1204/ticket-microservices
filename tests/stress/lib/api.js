import http from 'k6/http';
import { check, fail } from 'k6';
import { urls, password, jsonHeaders, num } from './config.js';

const SETUP_BATCH = num('STRESS_SETUP_BATCH', 10);
const TOKEN_MAX_AGE_MS = 45 * 60 * 1000;

const sessions = {};

function params(name, token) {
  const headers = token
    ? Object.assign({}, jsonHeaders, { Authorization: `Bearer ${token}` })
    : jsonHeaders;
  return { headers, tags: { name } };
}

function credentialsRequest(path, name, email) {
  return [
    'POST',
    `${urls.user}${path}`,
    JSON.stringify({ email, password }),
    params(name),
  ];
}

export function expectOk(res, label) {
  return check(res, { [`${label} answered 200`]: (r) => r.status === 200 });
}

export function createUsers(count, runId) {
  const users = [];
  for (let from = 0; from < count; from += SETUP_BATCH) {
    const emails = [];
    for (let i = from; i < Math.min(from + SETUP_BATCH, count); i++) {
      emails.push(`stress-${runId}-${i}@example.com`);
    }
    const registered = http.batch(
      emails.map((email) => credentialsRequest('/api/v1/auth/register', 'register', email)),
    );
    registered.forEach((res, i) => {
      if (res.status !== 201) {
        fail(`register ${emails[i]} answered ${res.status}: ${res.body}`);
      }
    });
    const logins = http.batch(
      emails.map((email) => credentialsRequest('/api/v1/auth/login', 'login', email)),
    );
    logins.forEach((res, i) => {
      if (res.status !== 200) {
        fail(`login ${emails[i]} answered ${res.status}: ${res.body}`);
      }
      users.push({ email: emails[i], token: res.json('token'), issuedAt: Date.now() });
    });
  }
  return users;
}

export function tokenFor(user) {
  const cached = sessions[user.email] || user;
  if (Date.now() - cached.issuedAt < TOKEN_MAX_AGE_MS) {
    return cached.token;
  }
  const [, url, body, requestParams] = credentialsRequest('/api/v1/auth/login', 'login', user.email);
  const res = http.post(url, body, requestParams);
  if (res.status !== 200) {
    return cached.token;
  }
  sessions[user.email] = { token: res.json('token'), issuedAt: Date.now() };
  return sessions[user.email].token;
}

export function createEvent({ name, rows, seatsPerRow }) {
  const startsAt = new Date(Date.now() + 30 * 24 * 3600 * 1000);
  const endsAt = new Date(startsAt.getTime() + 3 * 3600 * 1000);
  const res = http.post(
    `${urls.event}/api/v1/events`,
    JSON.stringify({
      name,
      description: 'k6 stress run',
      venue: 'Stress Arena',
      starts_at: startsAt.toISOString(),
      ends_at: endsAt.toISOString(),
      layout: {
        sections: [{ name: 'A', rows, seats_per_row: seatsPerRow, price_minor: 5000 }],
      },
    }),
    params('create_event'),
  );
  if (res.status !== 201) {
    fail(`create event answered ${res.status}: ${res.body}`);
  }
  return { id: res.json('event.id'), seatCount: res.json('seat_count') };
}

export function listEvents() {
  return http.get(`${urls.event}/api/v1/events?limit=20`, params('list_events'));
}

export function getEvent(eventId) {
  return http.get(`${urls.event}/api/v1/events/${eventId}`, params('get_event'));
}

export function listSeatsPage(eventId, offset) {
  return http.get(
    `${urls.event}/api/v1/events/${eventId}/seats?limit=100&offset=${offset}`,
    params('list_seats'),
  );
}

export function listProducts() {
  return http.get(`${urls.product}/api/v1/products?limit=20`, params('list_products'));
}

export function listBookingsPage(token, offset) {
  return http.get(
    `${urls.booking}/api/v1/bookings?limit=100&offset=${offset}`,
    params('list_bookings', token),
  );
}

export function createBooking(token, eventId, seatIds) {
  return http.post(
    `${urls.booking}/api/v1/bookings`,
    JSON.stringify({ event_id: eventId, seat_ids: seatIds }),
    params('create_booking', token),
  );
}

export function getBooking(token, bookingId) {
  return http.get(`${urls.booking}/api/v1/bookings/${bookingId}`, params('get_booking', token));
}

function readAll(fetchPage, what) {
  let rows = [];
  let offset = 0;
  for (;;) {
    const res = fetchPage(offset);
    if (res.status !== 200) {
      fail(`${what} answered ${res.status}: ${res.body}`);
    }
    const page = res.json();
    rows = rows.concat(page.data);
    if (!page.pagination.has_more || page.data.length === 0) {
      return rows;
    }
    offset += page.data.length;
  }
}

export function fetchSeats(eventId) {
  return readAll((offset) => listSeatsPage(eventId, offset), 'list seats');
}

export function listMyBookings(token) {
  return readAll((offset) => listBookingsPage(token, offset), 'list bookings');
}
