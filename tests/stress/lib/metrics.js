import { Counter, Trend } from 'k6/metrics';

export const sagaCompletion = new Trend('saga_completion_time', true);
export const bookingsConfirmed = new Counter('bookings_confirmed');
export const bookingsCancelled = new Counter('bookings_cancelled');
export const bookingRejected = new Counter('booking_rejected');
export const sagaUnresolved = new Counter('saga_unresolved');

export const invariantViolations = new Counter('invariant_violations');
export const invariantScrapeFailures = new Counter('invariant_scrape_failures');
export const sampledStuckPending = new Trend('sampled_stuck_pending');
export const sampledDbPoolInUse = new Trend('sampled_db_pool_in_use');
