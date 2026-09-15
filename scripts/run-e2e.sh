#!/bin/bash
# Runs the e2e/ suite against a SEPARATE, dedicated docker-compose project
# ("ticket-e2e", .env.e2e) -- never your normal dev stack. Every host port in
# .env.e2e is dev port + 10000, so the two run side by side without colliding,
# and this script never touches the containers, volumes, or Postgres data your
# main `docker compose up -d` stack holds. It's brought up once and left
# running (like the dev stack): e2e-test-writer's tests mint fresh, unique data
# per run and never assume a clean DB, so there's no need to reset it between
# runs -- and since it's a dedicated test project rather than your local
# environment, there's nothing to be careful about anyway.
#
# Usage:
#   scripts/run-e2e.sh                    # bring the e2e stack up if needed, wait, run the suite
#   scripts/run-e2e.sh -run TestFoo -v    # extra args are passed straight to `go test`
#   scripts/run-e2e.sh down               # tear the e2e stack down (add -v to also wipe its volumes)
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

PROJECT_NAME=ticket-e2e
ENV_FILE=.env.e2e
COMPOSE=(docker compose -p "$PROJECT_NAME" --env-file "$ENV_FILE")

if [ "${1:-}" = "down" ]; then
  shift
  echo "==> Tearing down the e2e stack ($PROJECT_NAME)"
  exec "${COMPOSE[@]}" down "$@"
fi

# shellcheck disable=SC1091
set -a; source "$ENV_FILE"; set +a

GATEWAY_URL="http://localhost:${KONG_PROXY_HOST_PORT}"
export GATEWAY_URL
export BOOKING_DATABASE_URL="postgres://booking_service:booking_service@localhost:${BOOKING_DB_HOST_PORT}/booking_service?sslmode=disable"
export EVENT_DATABASE_URL="postgres://event_service:event_service@localhost:${EVENT_DB_HOST_PORT}/event_service?sslmode=disable"
export ANALYTICS_DATABASE_URL="postgres://analytics_service:analytics_service@localhost:${ANALYTICS_DB_HOST_PORT}/analytics_service?sslmode=disable"
export USER_DATABASE_URL="postgres://user_service:user_service@localhost:${USER_DB_HOST_PORT}/user_service?sslmode=disable"
export KAFKA_BROKERS="localhost:${KAFKA_EXTERNAL_HOST_PORT}"
export KAFKA_CONNECT_URL="http://localhost:${KAFKA_CONNECT_HOST_PORT}/connectors"
export USER_SERVICE_HEALTHZ_URL="http://localhost:${USER_SERVICE_HOST_PORT}/healthz"
export EVENT_SERVICE_HEALTHZ_URL="http://localhost:${EVENT_SERVICE_HOST_PORT}/healthz"
export ANALYTICS_SERVICE_HEALTHZ_URL="http://localhost:${ANALYTICS_SERVICE_HOST_PORT}/healthz"
export BOOKING_SERVICE_HEALTHZ_URL="http://localhost:${BOOKING_SERVICE_HOST_PORT}/healthz"

echo "==> Bringing up the e2e stack ($PROJECT_NAME) if it isn't already running"
"${COMPOSE[@]}" up -d --build

HEALTHY_SERVICES=(postgres-user postgres-event postgres-analytics postgres-booking kafka kafka-connect kong)
ONESHOT_SERVICES=(kafka-init connect-init)
APP_HEALTHZ_URLS=(
  "$USER_SERVICE_HEALTHZ_URL"
  "$EVENT_SERVICE_HEALTHZ_URL"
  "$ANALYTICS_SERVICE_HEALTHZ_URL"
  "$BOOKING_SERVICE_HEALTHZ_URL"
)
CONNECTORS=(booking-service-outbox event-service-outbox user-service-outbox)
TIMEOUT_SECS=180 # first run builds images + creates a replication slot; can be slower than a plain restart

connector_ready() {
  curl -fsS "${KAFKA_CONNECT_URL}/$1/status" 2>/dev/null | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
if d.get("connector", {}).get("state") != "RUNNING":
    sys.exit(1)
tasks = d.get("tasks", [])
sys.exit(0 if tasks and all(t.get("state") == "RUNNING" for t in tasks) else 1)
'
}

echo "==> Waiting up to ${TIMEOUT_SECS}s for the e2e stack to become healthy"
deadline=$((SECONDS + TIMEOUT_SECS))
while true; do
  all_ready=true

  for svc in "${HEALTHY_SERVICES[@]}"; do
    health=$("${COMPOSE[@]}" ps --format '{{.Health}}' "$svc" 2>/dev/null || true)
    [ "$health" = "healthy" ] || all_ready=false
  done

  for svc in "${ONESHOT_SERVICES[@]}"; do
    state=$("${COMPOSE[@]}" ps -a --format '{{.State}} {{.ExitCode}}' "$svc" 2>/dev/null || true)
    case "$state" in
    "exited 0") ;;
    *) all_ready=false ;;
    esac
  done

  for url in "${APP_HEALTHZ_URLS[@]}"; do
    curl -fsS -o /dev/null "$url" 2>/dev/null || all_ready=false
  done

  for c in "${CONNECTORS[@]}"; do
    connector_ready "$c" || all_ready=false
  done

  if [ "$all_ready" = true ]; then
    echo "==> e2e stack is healthy"
    break
  fi

  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "==> Timed out after ${TIMEOUT_SECS}s waiting for the e2e stack to become healthy." >&2
    echo "    Current state:" >&2
    "${COMPOSE[@]}" ps -a >&2
    exit 1
  fi

  sleep 3
done

echo "==> Running the e2e suite against $PROJECT_NAME (go test -C e2e -tags=e2e -count=1 ./...)"
# -count=1 is required, not cosmetic: these tests are non-hermetic (real DB/Kafka state),
# so Go's test cache -- which only hashes source/binary/flags, not live stack state --
# would otherwise happily replay a stale "ok" from a previous run.
if [ "$#" -gt 0 ]; then
  exec go test -C e2e -tags=e2e -count=1 "$@"
else
  exec go test -C e2e -tags=e2e -count=1 ./...
fi
