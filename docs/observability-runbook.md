# Observability runbook: diagnosing the running stack

How to use the logs, metrics and alerts in this repo, day to day, to find out what is wrong with a
running service. It is written for the local `docker-compose.yml` stack and from the code and
config in this repo. Run every command from the repository root.

Read section 2 once. Use sections 3 to 6 while working. Sections 7 and 8 are for practising and
for explaining the work to someone else.

## 1. What you can see, and where

| Signal | Where | It answers |
|---|---|---|
| Metrics | Prometheus `http://localhost:9090`, Grafana `http://localhost:3000` (admin / admin unless `GRAFANA_ADMIN_*` is set) | Is something broken, how badly, since when |
| Alerts | Grafana → Alerting → Alert rules (8 rules, folders *Service health*, *Saga integrity*, *Kafka*) | What needs a human right now |
| Logs | `docker compose logs <service>` | Why one request or one message failed |
| Saga state | Each service's Postgres, plus the Kafka tools inside the `kafka` container | Where a booking stopped |
| Outbox publishing | Kafka Connect REST `http://localhost:8083` | Whether outbox events are leaving the database |

Host ports: Kong `8000` (admin `8001`), user-service `8081`, event-service `8082`,
analytics-service `8084`, booking-service `8085` (container `8083`), product-service `8086`.
Postgres: user `5433`, analytics `5434`, event `5435`, booking `5436`, product `5437`
(user, password and database are all `<service>_service`, for example `booking_service`).
Kafka is `kafka:9092` inside the network and `localhost:9094` from the host. Every service also
serves its own `/metrics` on its host port, for example `curl -s localhost:8082/metrics`.

What does **not** exist, so do not go looking: distributed traces, a log search backend (Loki or
ELK), consumer `processed / retried / dead-lettered` counters, and a request id that crosses
Kafka. Section 9 lists these and what to use instead.

## 2. The mental model

**Metrics tell you that and how much. Logs tell you why. State tells you where.** Always go in
that order.

1. An alert or a dashboard says *something is wrong* and gives you a service, a route or a
   topic. It never gives you the cause.
2. Logs for that service and time window give you one failing example, with a `request_id` or an
   `event_id`.
3. Database state and Kafka offsets for that one example tell you where a multi-step flow
   stopped.

### Request ids

- **Kong stamps every request** with `X-Request-Id` (global `correlation-id` plugin) unless the
  client sent one, and returns it in the response header. Keep it: it is the handle for
  everything below.
- Every service reuses that id in its access log line. Rust and Node ignore an inbound id that is
  longer than 128 characters or has characters outside `A-Za-z0-9._-`, and mint a new one.
- **The id stops at the Kafka boundary.** After the outbox write, follow the flow by
  `aggregate_id`, which is the `booking_id` for every event in the seat-reservation saga, or by
  `event_id`.

### Log shapes

| Service | One access-log line | Lines logged inside a request or message |
|---|---|---|
| event, analytics (Go) | `{"time","level","msg":"http_request","request_id","method","path","status","duration_ms",...}` | consumers add `event_id`, `booking_id` / `user_id` / `order_id`, `offset`, `component`, `group` at the top level |
| user, booking (Rust) | `{"timestamp","level","fields":{"message":"request completed","request_id","method","path","status","duration_ms"},"target"}` | `span` holds `{"name":"request","request_id"}` or, in a consumer, `{"name":"consume","group","topic","partition","offset","event_id","aggregate_id"}` |
| product (Node, pino) | `{"level","msg":"request completed","request_id","method","path","status","duration_ms"}` | |

Rust writes `request failed` at `ERROR` for a 5xx and `request completed` at `INFO` otherwise.
`/healthz`, `/readyz` and `/metrics` are not logged by the Rust and Node services. The Go
services log every `/metrics` scrape, so filter those out when reading (`grep -v '/metrics'`).

### Reading logs

```bash
docker compose logs --since 15m booking-service
docker compose logs -f --since 1m event-service
docker compose logs --since 1h 2>&1 | grep -F '<request-id-or-booking-id>'
```

The `grep -F` form searches every service at once and keeps the service name prefix, which is
usually the fastest way to follow one id. When you need structure, strip the prefix and use `jq`
(`-R 'fromjson?'` skips any non-JSON line instead of failing):

```bash
docker compose logs --no-log-prefix booking-service \
  | jq -R 'fromjson? | select(.level=="ERROR")'

docker compose logs --no-log-prefix booking-service \
  | jq -R 'fromjson? | select(.fields.request_id=="<id>" or .span.request_id=="<id>")'

docker compose logs --no-log-prefix booking-service \
  | jq -R 'fromjson? | select(.span.aggregate_id=="<booking-id>")'

docker compose logs --no-log-prefix event-service \
  | jq -R 'fromjson? | select(.booking_id=="<booking-id>")'
```

Which key to use: `.request_id` for Go and Node, `.fields.request_id` for a Rust access line,
`.span.request_id` for any other Rust line produced while handling that request.

## 3. Daily routine

### Start of the day (about 5 minutes)

```bash
docker compose ps
```
Everything `Up` or `healthy`. `kafka-init`, `outbox-ready` and `connect-init` are one-shots and
should show `Exited (0)`. If `outbox-ready` shows `Exited (1)`, a service did not create
`outbox_events` within five minutes (check that service's logs), and `connect-init` never ran.

```bash
curl -s -u admin:admin localhost:3000/api/prometheus/grafana/api/v1/rules \
  | jq -r '.data.groups[].rules[] | "\(.state)\t\(.health)\t\(.name)"'
```
Read it as follows. `firing` needs action. `nodata` means the rule has nothing to evaluate, so
check why before trusting it (section 4, "no data is not zero"). `inactive` and `ok` is healthy.

```bash
curl -s localhost:9090/api/v1/targets \
  | jq -r '.data.activeTargets[] | "\(.health)\t\(.labels.job)\t\(.lastError)"'
```
Every target `up`. A `down` target means that service's alerts have gone quiet. If
`kafka-exporter` is down, the lag and DLQ alerts show `nodata`; `docker compose up -d kafka-exporter`
brings it back.

```bash
for c in user-service booking-service event-service product-service; do
  curl -s localhost:8083/connectors/$c-outbox/status | jq -c '[.name, .connector.state, .tasks[].state]'
done
docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
docker compose exec -T kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic booking.events.dlq
```
Connectors `RUNNING`. Lag zero or falling, with a `CONSUMER-ID` on every row. DLQ offsets `0`
(`topic:partition:end-offset`; use `seat_reservation.events.dlq`, `user.events.dlq` and
`product_order.events.dlq` the same way).

The two money invariants, straight from the database (the same queries the booking-service
health reporter runs once a minute):

```bash
docker compose exec -T postgres-booking psql -U booking_service -d booking_service -c \
 "SELECT count(*) AS oversold FROM (SELECT 1 FROM bookings, unnest(seat_ids) AS seat_id
   WHERE status='confirmed' GROUP BY event_id, seat_id HAVING count(*)>1) x"
docker compose exec -T postgres-booking psql -U booking_service -d booking_service -c \
 "SELECT count(*) AS stuck FROM bookings
   WHERE status='pending' AND created_at < now() - interval '120 seconds'"
```
Both must be `0`. Grafana's *Service RED Metrics* dashboard is the last stop: pick a `$job`, look
at the last 6 hours for any route whose error rate or p95 stepped up.

### After you change code and redeploy

```bash
docker compose up -d --build <service>
docker compose logs --since 2m <service>
```
1. Startup lines are present: `listening` for HTTP services, `kafka consumer started` (Rust) or
   `consumer started` (Go) for each group.
2. Send one real request through Kong and follow its `X-Request-Id` (section 5).
3. Watch the dashboard for 5 minutes. A new 5xx band or a p95 step right after the deploy is
   yours.
4. Re-run the lag and DLQ checks above.

After editing `kong/kong.yml` run `docker compose restart kong`. After editing anything under
`monitoring/grafana/provisioning/` run `docker compose restart grafana`.

## 4. Incident method

1. **Classify the symptom (30 seconds).**
   - Callers get errors or slowness → *RED path*: dashboard by route, then logs.
   - A user did something and nothing happened, with no error → *saga path*: section 5.
   - An alert fired → its playbook in section 6.
2. **Scope it.** Which service (`job`), which route (`path`), which status. The dashboard has a
   `$job` variable; in Prometheus break the query down by `path` and `status`.
3. **Find the start time.** Zoom the graph to the first bad sample, then compare with the
   deploys: `docker inspect -f '{{.Name}} {{.State.StartedAt}}' $(docker compose ps -q)`.
4. **Get one failing example.** For a request, the `X-Request-Id` response header or an `ERROR`
   line. For a message, the `event_id` / `aggregate_id` on a `... -> dlq` or `retrying in place`
   line.
5. **Read everything that example touched** (`grep -F` across all services).
6. **Check the state** of the affected row (section 5, 6).
7. **Change one thing, then confirm with the same signal** that first showed the problem. If the
   metric has not moved back, the fix is not done.
8. **Write it down:** symptom, which signal showed it, cause, fix, and what alert or log line
   would have caught it sooner. That last line is how this stack improves.

### Metrics cookbook

Prometheus (`:9090/graph`) or Grafana → Explore → Prometheus.

```promql
sum by (job, path) (rate(http_requests_total[5m]))

100 * sum by (job, path) (rate(http_requests_total{status=~"5.."}[5m]))
    / sum by (job, path) (rate(http_requests_total[5m]))

topk(5, histogram_quantile(0.95,
  sum by (le, job, path) (rate(http_requests_duration_seconds_bucket[5m]))))

sum by (job, path, status) (rate(http_requests_total{job="booking-service"}[5m]))

db_pool_connections_in_use / db_pool_connections_max
sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag, 0))
sum by (topic) (kafka_topic_partition_current_offset{topic=~".+[.]dlq"}
              - kafka_topic_partition_oldest_offset{topic=~".+[.]dlq"})

bookings_oversold_seats
bookings_stuck_pending
probe_success{job="kafka-connect-connectors"}
up
```

Reading them correctly:

- **`rate()` for rates, `increase()` for "how many in this window".** Counters only go up and
  reset on restart; both functions handle the reset.
- **A latency percentile is computed from histogram buckets**, so it is only as precise as the
  bucket edges (5 ms to 10 s here). A p99 of exactly `10` means "at least the top bucket".
- **The `path` label is the route pattern** (`/api/v1/bookings/:id`), never a raw URL, so a
  UUID in a path cannot blow up the number of series.
- **No data is not zero.** A counter with labels does not exist until its first request after a
  restart, so a quiet service shows nothing, not `0`. An alert in `nodata` is not a healthy
  alert. The 5xx and p95 rules treat no data as OK on purpose (a quiet route must not page).
- **Check `up` first** when a graph goes flat: a dead service stops reporting and everything
  built on its metrics goes quiet with it.
- The exporter metrics (`kafka_*`) come from `kafka-exporter`; if that target is down, the lag
  and DLQ rules show `nodata`.

## 5. Following one flow end to end

### A request

```bash
curl -si -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"event_id":"<event-uuid>","seat_ids":["<seat-uuid>"]}' \
  localhost:8000/api/v1/bookings | tee /tmp/booking-response.txt | head -20
```
The response carries `X-Request-Id: <id>` and, in the body, the booking `id`. Event and seat ids
come from event-service (`http://localhost:8082/swagger/`); login examples are in
`docs/curl-examples.md`.

```bash
docker compose logs --since 5m 2>&1 | grep -F '<request-id>'
```
You should see one `request completed` line from booking-service with the status and
`duration_ms`. If it is a 5xx, the `ERROR` lines for the same request carry the same id in
`span.request_id`.

### The saga for that booking (bisect it)

The booking is `pending` until `event-service` replies. Find the first hop that did not happen;
the failure is at or just after it. Outbox rows are inserted and deleted in the same
transaction by design, so an empty `outbox_events` table does **not** mean events were lost.

| # | Hop | How to check |
|---|---|---|
| 1 | Booking row exists | `docker compose exec -T postgres-booking psql -U booking_service -d booking_service -c "SELECT id,status,failure_reason,created_at FROM bookings WHERE id='<booking-id>'"` |
| 2 | `BookingRequested` left the booking DB | `curl -s localhost:8083/connectors/booking-service-outbox/status` is `RUNNING`; `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots` (booking DB) shows `active = t` and a small lag |
| 3 | It is on `booking.events` | `docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic booking.events --from-beginning --timeout-ms 5000 --property print.key=true --property print.headers=true \| grep <booking-id>` (the key is the booking id; headers carry `event_type` and `event_id`) |
| 4 | event-service consumed it | `docker compose logs event-service 2>&1 \| grep <booking-id>` shows `seat reservation processed` (or a retry / `-> dlq`); group lag `event-service-BookingRequested` is `0` |
| 5 | Seat was reserved | `docker compose exec -T postgres-event psql -U event_service -d event_service -c "SELECT * FROM seat_reservations WHERE booking_id='<booking-id>'"` shows `held` |
| 6 | `SeatReserved` / `SeatReservationFailed` is on `seat_reservation.events` | same console-consumer command, topic `seat_reservation.events` |
| 7 | booking-service consumed it | `docker compose logs booking-service 2>&1 \| grep <booking-id>` shows `saga event processed` with `span.aggregate_id` equal to the booking id; group lag `booking-service-SeatReserved` is `0` |
| 8 | Row is final | step 1 again: `confirmed`, or `cancelled` with a `failure_reason` |

`failure_reason = reservation_timeout` means the booking-service reaper cancelled it because no
reply arrived within the pending timeout (120 s by default).

## 6. Alert playbooks

Each rule lives in `monitoring/grafana/provisioning/alerting/`. Severity is a label:
`page` needs action now, `warning` can wait for working hours.

### Booking or login routes returning 5xx (page)

More than 5% of requests to `/api/v1/bookings*`, `/api/v1/auth/login` or `/api/v1/auth/register`
returned 5xx for 5 minutes, with at least 10 requests in that window.

1. The alert labels name the `job` and `path`.
2. Read that service's errors: `docker compose logs --since 15m <svc>` and look for
   `request failed` (Rust, Node) or `"status":5` (Go). Take one `request_id`.
3. Read the lines around it with the same id, then `curl -s localhost:<port>/readyz`: a failing
   readiness check means the database is unreachable.
4. Look at the pool (below) and at `docker compose ps` for a restarting dependency.

Usual causes: Postgres down or restarting, pool exhausted, a bad deploy, a schema mismatch after
a migration. Confirm recovery on the same graph.

### Booking or login routes slow (warning)

p95 above 1 s for 5 minutes. Find the route on the p95 panel, then check, in order: the pool
saturation panel (queuing for a connection looks exactly like a slow query); long statements
(`SELECT pid, now()-query_start AS age, state, wait_event_type, left(query,80) FROM pg_stat_activity WHERE datname=current_database() ORDER BY age DESC LIMIT 10`);
container CPU (`docker stats --no-stream`). Login does an Argon2 verification, which is
deliberately CPU-heavy, so expect it to be the slowest route; compare with its own history before
calling it a regression.

### DB connection pool nearly full (warning)

More than 80% of connections checked out for 5 minutes. Once it is full requests queue and
latency climbs.

1. Which service: the `job` label. Go services also expose
   `rate(db_pool_empty_acquire_wait_seconds_total[5m])`, the time spent waiting for a free
   connection; the Rust services do not.
2. Look for work holding a connection:
   `SELECT state, count(*) FROM pg_stat_activity WHERE datname=current_database() GROUP BY 1`.
   Many `idle in transaction` means a code path is holding a transaction open across slow work.
   Non-database work belongs before `Begin`, not inside the transaction.
3. Do not just raise `DB_MAX_CONNECTIONS`; five databases share one Postgres server's limits.
   Find the long transaction first.

`nodata` here means the `db_pool_*` series are absent; check the scrape target.

### DLQ has messages (page)

A `<topic>.dlq` holds a message, so a saga step did not complete.

1. Which topic: the alert label. Read the records with their diagnostic headers:
   ```bash
   docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic booking.events.dlq --from-beginning --timeout-ms 5000 \
     --property print.key=true --property print.headers=true
   ```
2. Read `x-dlq-reason`. Its prefix is the diagnosis: `parse:` is a poison message
   (undeserializable or empty), `permanent:` is a domain or constraint rejection (the SQLSTATE
   is included), `max-retries after N attempts:` is a transient error that never cleared.
   `x-dlq-source-topic`, `-partition` and `-offset` give the original position.
3. Find the consumer's own log line for it: `message dead-lettered` plus the `event_id` and
   offset, and the lines just before it.
4. Fix the cause. **There is no replay tool.** After the fix, re-publish the record's payload
   with the same key, `event_type` and `event_id` headers to the source topic; the idempotent
   consumer makes a duplicate harmless.
5. The alert measures `log-end offset − oldest retained offset`, so it keeps firing until
   retention drops the records, even after the fix. To clear it in a dev stack, delete the
   records (`kafka-delete-records.sh` with offset `-1`, which means "up to the end") or recreate
   the topic.

### Consumer lag not draining (warning)

A group's committed lag never reached zero in 10 minutes.

```bash
docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group booking-service-SeatReserved
```
- `CONSUMER-ID` is `-` on every row: nothing is consuming. Check the service is up and logged
  `kafka consumer started` for that group.
- `CURRENT-OFFSET` does not move while the log shows `message not processed; retrying in place`
  or `transient error, backing off` for one offset: the consumer is retrying a single message and
  deliberately not committing past it. That is a wedged partition. Read the `err` field, fix or
  unblock the dependency, or let it reach the DLQ.
- `CURRENT-OFFSET` moves but `LAG` grows: it is a throughput problem, not a failure.

### Seat sold twice (page)

`bookings_oversold_seats` is above zero: two confirmed bookings hold the same seat. This is a
correctness failure, not an operational one. Stop and investigate before anything else.

```sql
SELECT event_id, seat_id, array_agg(id) AS bookings
FROM bookings, unnest(seat_ids) AS seat_id
WHERE status = 'confirmed'
GROUP BY event_id, seat_id HAVING count(*) > 1;
```
Then, for each colliding booking, run the bisect in section 5 and look at event-service's
`seat_reservations` and `seats`. The prime suspects are the reserve transaction in event-service
and the idempotency check on `processed_events`. The `review-concurrency` skill audits exactly
this.

### Bookings stuck pending (page)

`bookings_stuck_pending` is above zero for 2 minutes: pending bookings older than the timeout
plus two reaper intervals. The reaper should have cancelled them.

1. List them:
   `SELECT id, created_at FROM bookings WHERE status='pending' AND created_at < now() - interval '120 seconds'`.
2. Is the reaper alive: `docker compose logs booking-service | grep -E 'reaper (started|tick)|cancelled stale'`.
3. If the reaper is alive and the rows stay pending, the cancel path is failing too: look for
   `ERROR` lines from it.
4. Either way, bisect one booking through the saga (section 5) to find the hop that never
   happened.

### Debezium connector not running (page)

The connector or its task is `FAILED`, `PAUSED`, `UNASSIGNED`, missing, or Kafka Connect is
unreachable. While it is down no outbox event for that service reaches Kafka, so its saga steps
silently stop.

```bash
curl -s localhost:8083/connectors/<name>/status | jq
curl -s -X POST 'localhost:8083/connectors/<name>/restart?includeTasks=true&onlyFailed=true'
```
If the task is `FAILED`, the `trace` field has the reason. Then check the replication slot on
that service's database (`SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots`):
an **inactive slot keeps WAL on disk**, so a connector left down for long enough fills the
database's disk.

In this dev stack `product-service-outbox` is down, and this alert fires, whenever the product
database or service is not running. That is expected, not a fault.

## 7. Practise on purpose (game days)

Break the local stack deliberately and watch what shows up. Each drill is also an honest story to
tell. These are written from the config and code; the first time you run one, check the result
against the text and fix this document where reality differs.

**Drill 1: stop the outbox publisher.**
`curl -X PUT localhost:8083/connectors/booking-service-outbox/pause`. Create a booking.
Expect: the connector probe goes to `0`, *Debezium connector not running* fires after about two
minutes, the booking stays `pending`, and the reaper cancels it at about 120 s with
`failure_reason = reservation_timeout`. Recover with `PUT .../resume`.
What you learn: how long a silent saga stall takes to be noticed, and that the reaper is the
safety net.

**Drill 2: take a consumer's service away.**
`docker compose stop event-service`, create a booking, wait. Expect: the booking stays `pending`
and the lag for `event-service-BookingRequested` grows; the reaper cancels the booking. Left for
10 minutes (and with `kafka-exporter` running), *Consumer lag not draining* fires. Start the
service again and watch the lag drain.
What you learn: the difference between a stall you can see in lag and a stall you can only see in
state.

**Drill 3: a poison message.**
```bash
docker compose exec -T kafka kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic seat_reservation.events <<< 'not-json'
```
Expect: both booking-service consumer groups dead-letter it (it carries no `event_type` header,
so neither skips it), *DLQ has messages* fires, and `x-dlq-reason` starts with `parse:`. Practise
the section 6 DLQ playbook, then delete the records to clear the alert.
What you learn: poison versus transient versus permanent, and why the alert does not self-clear.

**Drill 4: lose the database.**
`docker compose stop postgres-booking`, then send 20 requests to `/api/v1/bookings` in a loop.
Expect several alerts at once (5xx, connector, readiness). Practise telling the cause from the
consequences: the database is the cause, everything else follows from it. Restart it and confirm
every signal returns to normal.
What you learn: alert storms, and reading the dependency order.

After each drill write the section 4, step 8 note. Three of those notes are a better answer to
"tell me about a time you debugged a production problem" than any amount of dashboards.

## 8. Explaining this work honestly

What is accurate to say:

- Every service exposes the same RED metrics with identical names and labels, scraped by
  Prometheus and shown on one provisioned Grafana dashboard.
- There are eight alert rules. Two of them watch business invariants (no seat sold twice, no
  booking stuck pending), not just CPU or errors. Four watch the asynchronous path that HTTP
  metrics cannot see: DLQ depth, consumer lag, connector health and pool saturation.
- Every request has an id from the gateway through each service's JSON access log, and every
  consumed message is logged with its `event_id` and `aggregate_id`.
- Delivery is at-least-once with an idempotent consumer, so a failure shows up as a retry, a
  duplicate-skipped line or a DLQ record, each with a distinct log line and reason prefix.

What not to say: that there is distributed tracing, that a request id follows a saga across
Kafka, that logs are centrally searchable, or that real defects were found *in production* from
these signals. The bugs in this repo's history (a DLQ message-loss bug, a reaper N+1) were found
by tests and review. The drills in section 7 are where the "I diagnosed it from logs and metrics"
stories honestly come from.

A 90-second answer to "how do you debug a failing distributed flow":

> I start from the symptom, not the code: which alert fired or which route's error rate or p95
> moved, and when. That gives me a service and a window. I pull one failing example, a
> `request_id` for an HTTP call or the booking id for a saga, and read every log line it
> touched. For the asynchronous part the request id stops at the outbox, so I bisect by booking
> id across the database, the Debezium connector and slot, the topic, the consumer group's lag
> and the DLQ until I find the first hop that did not happen. Then I change one thing and watch
> the same signal that showed the problem come back.

Questions to be ready for:

- *Why alert on DLQ depth and stuck-pending, not just 5xx?* Because the saga fails silently:
  nothing returns an error to a caller, so only a state or queue signal can see it.
- *Why is `path` a route pattern?* To keep metric cardinality bounded.
- *Counter, gauge or histogram for each of these?* Requests and DLQ writes are counters, pool
  usage and lag are gauges, latency is a histogram so you can compute percentiles after the fact
  and aggregate them across instances.
- *How does a duplicate event show up?* A `duplicate event skipped` log line and no state change,
  because the consumer checks `processed_events` in the same transaction as the side effect.
- *What would you add next?* Section 9.

## 9. Known gaps and what to use instead

| Gap | Use instead | Fix |
|---|---|---|
| No distributed tracing | `request_id` for HTTP, `aggregate_id` / `event_id` for the saga | Add OTel as described in the `add-observability` skill, including the `tracecontext` outbox column and a `traceparent` Kafka header |
| No request id across Kafka | `aggregate_id` (the booking id) | Carry it in an outbox column and a Debezium header |
| No log search | `docker compose logs … \| grep` / `jq` | Loki and Promtail in compose |
| No consumer counters | lag and DLQ depth from `kafka-exporter`, and the log lines | `processed`, `retried`, `dead_lettered` counters per group |
| No alert on `up == 0` | the targets check in section 3 | An alert rule on `up == 0`; today a dead service makes its own alerts go quiet |
| No DLQ replay tool | re-publish by hand (section 6) | A small replay script |
| Go services log every `/metrics` scrape | `grep -v /metrics` | Skip the quiet paths as the Rust and Node services do |

The runbook is only as good as the stack under it. When a drill or an incident shows that a
signal was missing, add it and update this table.
