const host = __ENV.STRESS_HOST || 'localhost';
const direct = (__ENV.STRESS_TARGET || 'gateway') === 'direct';
const local = (port) => `http://${host}:${port}`;
const gateway = __ENV.STRESS_GATEWAY_URL || local(__ENV.STRESS_KONG_PORT || 8000);

export const urls = {
  user: direct ? __ENV.STRESS_USER_URL || local(8081) : gateway,
  event: direct ? __ENV.STRESS_EVENT_URL || local(8082) : gateway,
  booking: direct ? __ENV.STRESS_BOOKING_URL || local(8085) : gateway,
  product: direct ? __ENV.STRESS_PRODUCT_URL || local(8086) : gateway,
  bookingMetrics: __ENV.STRESS_BOOKING_METRICS_URL || local(8085),
};

export const password = 'correct-horse-battery-staple';

export const jsonHeaders = { 'Content-Type': 'application/json' };

export function num(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be a number, got ${raw}`);
  }
  return value;
}

export function text(name, fallback) {
  const raw = __ENV[name];
  return raw === undefined || raw === '' ? fallback : raw;
}

export function seconds(duration) {
  const match = /^(\d+)(s|m|h)$/.exec(duration);
  if (!match) {
    throw new Error(`expected a duration like 30s, 5m or 1h, got ${duration}`);
  }
  return Number(match[1]) * { s: 1, m: 60, h: 3600 }[match[2]];
}
