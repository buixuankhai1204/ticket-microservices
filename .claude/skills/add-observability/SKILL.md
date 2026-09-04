---
name: add-observability
description: Give one service the repo's full observability baseline — per-request structured access logs with an X-Request-Id generated/propagated on every request, OpenTelemetry traces (HTTP server span continuing the inbound W3C traceparent, spans per use case and repo call), W3C trace context carried across the outbox→Debezium→Kafka→consumer boundary so a client action is one trace end to end, and RED + consumer-lag/DLQ metrics on a Prometheus /metrics endpoint. Use to retrofit a service that predates this baseline (user-service has no per-request logging) or to add the cross-Kafka trace link a plain scaffold doesn't wire.
argument-hint: "<service-name>"
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add the observability baseline to a service

## Context
- Project conventions: @CLAUDE.md
- Reference request-ID + access-log middleware (Go): `services/analytics-service/internal/adapter/http/middleware.go`
- Target service: !`ls -R services/$1/internal services/$1/src 2>/dev/null | head -60`
- Does it publish/consume Kafka?: !`grep -rln "outbox_events\|kafka\|rdkafka" services/$1 2>/dev/null | head`
- Debezium connector for it: !`ls debezium/$1-outbox.json 2>/dev/null || echo "(none)"`

## Arguments
`$ARGUMENTS` — one `<service-name>` under `services/`.

## The four pieces (do all that apply)

### 1. Per-request structured logging + `X-Request-Id`  (every service)
An HTTP middleware pair, applied to **all** routes:
- **Request ID** — read inbound `X-Request-Id`; if absent, generate a UUID. Echo it in the
  response header and put it on the request context / task-local so every downstream log line
  and outbound call carries it.
- **Access log** — exactly one structured JSON line per request:
  `{"msg":"http_request","request_id","method","path","status","duration_ms"}`. Never log the
  `Authorization` header, a raw JWT, or the request body (see `security-reviewer`).

- **Go** (`event-service`, `analytics-service`, `booking-service` if Go): copy the
  `RequestID()` + `AccessLog()` pattern from `analytics-service`'s `middleware.go` into the
  target's `internal/adapter/http/middleware.go` and chain it in `NewRouter(...)`. If the
  service already has it, just confirm it's wired for every route group.
- **Rust / axum** (`user-service`): add a `tower_http::trace::TraceLayer` **plus** a small
  middleware that does the `x-request-id` read/generate/echo and puts it in a `tracing::Span`
  field, so the existing `tracing_subscriber` JSON output carries `request_id`, `method`,
  `path`, `status`, `latency` for `/api/v1/auth/*` and `/api/v1/users/*`. `user-service`
  currently logs only at startup/shutdown — this is the retrofit called out in the repo's
  logging convention.

### 2. OpenTelemetry tracing  (every service)
- Deps: Go — `go.opentelemetry.io/otel`, `otel/sdk`, `otel/exporters/otlp/otlptrace/otlptracehttp`,
  `otelhttp`. Rust — `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`,
  `tracing-opentelemetry`.
- Init a `TracerProvider` in `platform/` from env (`OTEL_EXPORTER_OTLP_ENDPOINT`,
  `OTEL_SERVICE_NAME=<service-name>`, `OTEL_TRACES_SAMPLER`), OTLP/HTTP exporter, batch
  processor; shut it down in the graceful-shutdown path (flush spans before exit).
- **HTTP server span** — wrap the router so each request gets a span that **extracts the W3C
  `traceparent`/`tracestate` from the inbound headers first** (Kong / the client started the
  trace; the service continues it, doesn't root a new one). Set `request_id` as a span
  attribute so logs and traces join.
- **Span per use case** (`Execute`/`execute`) and **per repository call**, so a slow query is
  visible. Keep the transaction boundary spans tight — the `Begin`→`Commit` window should be
  a visible child span (it's the pooled-connection hold time).
- Reuse the logger's request id as the span's `request_id` attribute; do **not** put PII
  (email, tokens) in span attributes.

### 3. Trace context across the async saga boundary  (only if the service publishes/consumes)
Without this, a booking's trace stops at "wrote outbox row" and a *new* unrelated trace
starts at "consumed BookingRequested" — the saga can't be followed end to end. Carry the W3C
trace context as **event metadata in a header**, not in the business payload:

- **Migration** (`/new-migration`): add `tracecontext TEXT` (nullable) to `outbox_events`.
- **Publish side** — when the use case writes the outbox row (`WriteOutbox`), serialize the
  current span's context with the W3C `TraceContext` propagator into that column
  (`traceparent` value, plus `tracestate` if present).
- **Debezium connector** (`debezium/<service>-outbox.json`) — extend
  `transforms.outbox.table.fields.additional.placement` to also map the column to a header:
  `event_type:header:event_type,id:header:event_id,tracecontext:header:traceparent`. A new
  `aggregate_type` still auto-routes; this just adds the header on every routed message.
- **Consume side** — in the consumer, before calling the use case, read the `traceparent`
  Kafka message header and **extract** it into the context, then start the "process event"
  span as a child of (or link to) that remote context. Now Kong→booking→Kafka→event→Kafka→
  booking is one trace.
- If `/design-saga` produced a design for this flow, note there that trace context rides the
  `traceparent` header.

### 4. Metrics  (every service)
- A Prometheus `/metrics` endpoint (Go: `promhttp`; Rust: `metrics-exporter-prometheus`),
  **separate** from `/healthz` and `/readyz`, unauthenticated but bound to the service port
  (Kong doesn't route to it).
- **RED** on HTTP: request count, error count (5xx), duration histogram — labelled by route
  and status class, low cardinality (no user id, no raw path with ids in it — use the route
  pattern).
- **Consumer metrics** (if it consumes): messages processed / skipped / retried / dead-
  lettered per topic+group, handler duration, and consumer lag if the client exposes it.
  DLQ-write count is a page-worthy signal — make sure it's a distinct counter.

## Instructions

1. Inventory what the service already has (steps above have `!` probes; also grep for
   `otel`, `promhttp`, `TraceLayer`). Only add what's missing.
2. Apply pieces 1, 2, 4 always; piece 3 only if the service touches Kafka.
3. `docker-compose.yml`: add an `otel-collector` service (`otel/opentelemetry-collector` with
   a minimal `otelcol.yaml` exporting to logs or to a backend if one is present) on
   `ticket-network`, and set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` +
   `OTEL_SERVICE_NAME` on the target service. Add `OTEL_*` to `.env.example`.
4. Keep it framework-agnostic where the code is: `domain` and `usecase` use the existing
   logger interface and `otel`'s global tracer (no exporter type in a signature); the
   exporter/provider lives in `platform/` and is wired in the composition root only.
5. `go build ./...` / `cargo build` + `cargo clippy -- -D warnings`. Confirm the
   `clean-architecture-check.sh` hook stays green (tracing is cross-cutting; the SDK setup is
   `platform/`-only).
6. Hand off: list what was added per piece, the new env vars, and — if piece 3 ran — that
   the `outbox_events` migration and the connector JSON both changed and need redeploying
   together. Note `integration-test-writer` / `e2e-saga-tester` can assert a single trace id
   spans the saga.
