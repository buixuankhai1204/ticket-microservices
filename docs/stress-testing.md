# Stress testing with k6

`tests/stress/` holds three [k6](https://k6.io) scenarios driven by `scripts/run-stress.sh`. They
sit outside the six test tiers in `CLAUDE.md`: they need the full compose stack running
(`docker compose up -d`) and they test capacity and invariants under load, not behaviour.

| Scenario | Script | What it proves |
|---|---|---|
| `smoke` | `smoke.js` | Every read path answers (events, seats, products, JWT-protected bookings list) at a steady rate, and the script itself works. 1 minute, 20 iterations/s. |
| `contention` | `seat-contention.js` | The seat-reservation saga under contention: many users race for few seats (default 50 users, 20 seats, ramp to 50 attempts/s). No seat is double-confirmed or oversold, held seats are released, every booking reaches `confirmed` or `cancelled`. |
| `soak` | `soak.js` | 30 minutes of mixed reads plus a steady trickle of bookings on unique seats. Latency stays flat, nothing is cancelled, invariants stay clean, DB pool use is sampled. |

## Running

```bash
brew install k6
scripts/run-stress.sh smoke
scripts/run-stress.sh contention
STRESS_DURATION=1h scripts/run-stress.sh soak
```

Without `k6` installed the wrapper falls back to `docker run grafana/k6` (override the image with
`STRESS_K6_IMAGE`). Arguments after `--` go to `k6 run`, for example
`scripts/run-stress.sh contention -- --out json=result.json`.

### Kong rate limits

Kong's per-route limits (60 to 1000 requests per minute) would answer almost every request with
`429`. By default the wrapper therefore:

1. rewrites every `minute:` value in `kong/kong.yml` to `100000000` into `tests/stress/.generated/kong.yml`
   (git-ignored, so the real file is never edited and cannot drift),
2. recreates only the `kong` container with that file mounted through
   `tests/stress/docker-compose.stress.yml`, and waits until `X-RateLimit-Limit-Minute` shows the lifted value,
3. runs k6,
4. recreates `kong` with the real `kong/kong.yml` again on exit, including on failure or Ctrl-C.

`scripts/run-stress.sh kong-lift` and `kong-restore` do steps 1 to 2 and 4 by hand.
`--keep-rate-limits` runs against the real limits (use `STRESS_USERS` of 25 or fewer: setup registers
and logs in every user, and `/api/v1/auth` allows 60 requests per minute). `--direct` skips Kong and
calls the services on their host ports (8081, 8082, 8085, 8086).

## Knobs

All are environment variables; every one is prefixed `STRESS_` so the docker fallback can pass them through.

| Variable | Default | Meaning |
|---|---|---|
| `STRESS_RATE` | smoke 20, contention 50 | Iterations per second (contention: peak booking attempts per second) |
| `STRESS_DURATION` | smoke `1m`, soak `30m` | Length of the steady phase |
| `STRESS_RAMP_UP` / `STRESS_HOLD` | `30s` / `1m` | Contention ramp and hold |
| `STRESS_USERS` | contention 50, soak 30 | Users registered and logged in during setup |
| `STRESS_SEATS` | contention 20, soak sized to the bookings | Seats on the created event |
| `STRESS_SEATS_PER_BOOKING` | `1` | 1 to 20; more than 1 gives partial-overlap contention |
| `STRESS_READ_RATE` / `STRESS_BOOKING_RATE` | `30` / `1` | Soak read and booking iterations per second |
| `STRESS_MAX_VUS` | `max(200, rate*20)` | Contention VU ceiling; raise it if `dropped_iterations` is not 0 |
| `STRESS_P95_READ_MS` / `STRESS_P95_CREATE_MS` / `STRESS_P95_SAGA_MS` | `500` / `1000` / `10000` | p95 thresholds (reads, `POST /bookings`, time to a final booking state) |
| `STRESS_SAGA_TIMEOUT_S` | `60` | How long one booking may stay `pending` before it counts as unresolved |
| `STRESS_DRAIN_TIMEOUT_S` | `240` | How long the final check waits for the saga to settle (the reaper cancels at 120 s) |
| `STRESS_TARGET`, `STRESS_HOST`, `STRESS_GATEWAY_URL`, `STRESS_*_URL`, `STRESS_BOOKING_METRICS_URL` | gateway, `localhost`, ports as in `docker-compose.yml` | Where to send traffic and where to scrape booking-service `/metrics` (host port 8085) |

## Reading the result

k6 exits non-zero when a threshold fails; the summary is also written to
`tests/stress/.generated/<scenario>-summary.json`.

- `dropped_iterations` above 0 means k6 ran out of VUs: the generator, or a slow stack, could not
  keep the arrival rate. Raise `STRESS_MAX_VUS` or lower `STRESS_RATE` before trusting latencies.
- `saga_completion_time` is `POST /bookings` until `GET /bookings/{id}` shows `confirmed` or
  `cancelled`. `saga_unresolved` counts bookings still `pending` after `STRESS_SAGA_TIMEOUT_S`.
- `bookings_cancelled{reason:seat_unavailable}` is the expected loser in `contention`; in `soak`
  any cancellation fails the run.
- `invariant_violations` must be 0. While the test runs, a sampler scrapes
  `bookings_oversold_seats` from booking-service. After the load stops, `teardown` waits for the
  saga to drain, then checks through the API that no seat is confirmed by two bookings, the seats
  `booked` equal the seats of confirmed bookings, none is left `reserved`, no booking is still
  `pending`, and `bookings_stuck_pending` is 0. Each failure prints its kind
  (`double_confirmed_seat`, `seat_state_mismatch`, `held_seats_not_released`, `pending_booking`,
  `stuck_pending`, `oversold_seats`).
- `invariant_scrape_failures` above 0 means the metrics URL is wrong or unreachable, so the
  server-side checks did not run.

Watch the Grafana "Service RED Metrics" dashboard and consumer lag / DLQ depth (`docs/observability-runbook.md`)
alongside the run; k6 only sees the edge. A soak that passes its thresholds but shows lag, pool use or
latency creeping upward in Grafana is still a finding.

## Notes

- The JWT lives 1 hour. Each k6 VU re-logs in after 45 minutes, so a soak longer than an hour keeps working.
- Every run creates its own users and event (`stress-<run id>`), so reruns do not collide. Data is
  not cleaned up; wipe the compose volumes to reset.
- Setup registers users with Argon2 hashing, which is CPU-heavy: keep `STRESS_USERS` modest.
- Do not point this at a shared or production stack.
