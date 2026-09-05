# Debezium connectors

Kafka Connect connector definitions for the CDC-based transactional outbox. The
`kafka-connect` service in `docker-compose.yml` runs `debezium/connect` (which
bundles the PostgreSQL connector and the Outbox Event Router SMT), and the
`connect-init` one-shot `POST`s each file here to the Connect REST API on
startup (an existing connector — HTTP 409 — is treated as success).

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
