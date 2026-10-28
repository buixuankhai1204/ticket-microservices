# Debezium connectors

Kafka Connect connector definitions for the CDC-based transactional outbox. The
`kafka-connect` service in `docker-compose.yml` runs `debezium/connect` (which
bundles the PostgreSQL connector and the Outbox Event Router SMT), and the
`connect-init` one-shot `POST`s each file here to the Connect REST API on
startup (an existing connector — HTTP 409 — is treated as success).

## Startup order

A connector needs `public.outbox_events` to exist when its task starts, and the
table is created by each service's own migrations at startup. The `outbox-ready`
one-shot (`wait-for-outbox.sh`) therefore polls every database listed in its
`OUTBOX_DATABASES` until the table exists (`OUTBOX_READY_TIMEOUT`, default 300 s,
then it fails and `connect-init` does not run); only then does `connect-init`
register the connectors. Publishing services do not wait for `connect-init`,
because they have to migrate before it can run.

- On a fresh volume a replication slot only exists once its connector first
  starts, so an outbox row written in the few seconds between the table appearing
  and the connector registering is not captured. Nothing writes in that window on a
  fresh stack. On an existing volume the slot already exists and keeps the WAL, so
  nothing is lost.
- Adding a publishing service: add its `debezium/<service>-outbox.json` to
  `connect-init` and its database to `OUTBOX_DATABASES`.
- A task that is already `FAILED` stays failed (a re-run of `connect-init` only
  sees HTTP 409). Restart it with
  `curl -X POST 'localhost:8083/connectors/<name>/restart?includeTasks=true&onlyFailed=true'`.

## `user-service-outbox.json`

Tails `public.outbox_events` in the `user_service` database via a logical
replication slot (`pgoutput`, `wal_level=logical` is set on `postgres-user`) and
routes each insert through the `EventRouter` SMT:

| Outbox column   | Becomes                                              |
|-----------------|-----------------------------------------------------|
| `aggregate_id`  | Kafka message **key** (string) — preserves ordering |
| `aggregate_type`| topic: `<aggregate_type>.events` (e.g. `user.events`) |
| `event_type`    | Kafka header `event_type`                            |
| `id`            | Kafka header `event_id`                              |
| `payload`       | Kafka message **value** (the JSON object, unwrapped) |

`skipped.operations=u,d,t` — only inserts are published, so the paired
`DELETE` the app issues in the same transaction (to keep the table empty) is
ignored on the wire.

### Credentials

`database.user` / `database.password` are the local-dev defaults from
`docker-compose.yml`. For any real environment, externalize them with a Connect
`config.providers` (e.g. `FileConfigProvider` or env) instead of inlining.

### Operating notes

- A stopped connector (or a Kafka outage) parks the replication slot, so
  `postgres-user` retains WAL until it catches up. Monitor
  `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;`
  and consider `max_slot_wal_keep_size` as a safety cap. While the connector is
  running, walsender keepalives advance `confirmed_flush_lsn` even when the
  outbox is idle. For a production deployment with bounded disk, also set
  `heartbeat.interval.ms` (and create its `__debezium-heartbeat.userdb` topic,
  since broker auto-create is off here).
- Connector status: `curl -s localhost:8083/connectors/user-service-outbox/status`.

## `booking-service-outbox.json`

Same shape as `user-service-outbox.json`, pointed at `postgres-booking` /
`booking_service` (`slot.name=booking_service_outbox`,
`publication.name=dbz_booking_outbox`, `topic.prefix=bookingdb`).
`aggregate_type=booking` routes to `booking.events`. Status:
`curl -s localhost:8083/connectors/booking-service-outbox/status`.

## `event-service-outbox.json`

Same shape as `user-service-outbox.json`, pointed at `postgres-event` /
`event_service` (`slot.name=event_service_outbox`,
`publication.name=dbz_event_outbox`, `topic.prefix=eventdb`).
`aggregate_type=seat_reservation` routes to `seat_reservation.events`. Status:
`curl -s localhost:8083/connectors/event-service-outbox/status`.
