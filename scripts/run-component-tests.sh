#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:-all}"

cd "$root"

run_event() {
  (cd services/event-service && go test -tags component -count=1 -timeout 10m ./internal/app/)
}

run_booking() {
  (cd services/booking-service && cargo test --test component -- --ignored --test-threads=4)
}

run_user() {
  (cd services/user-service && cargo test --test component -- --ignored --test-threads=6)
}

cleanup() {
  docker compose --profile component-test --profile gateway-test rm -sfv postgres-test wiremock >/dev/null 2>&1 || true
}

trap cleanup EXIT
docker compose --profile component-test --profile gateway-test up -d --wait postgres-test wiremock kafka

case "$target" in
  event) run_event ;;
  booking) run_booking ;;
  user) run_user ;;
  all)
    run_event
    run_booking
    run_user
    ;;
  *)
    echo "usage: $0 [event|booking|user|all]" >&2
    exit 2
    ;;
esac
