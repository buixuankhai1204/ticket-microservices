#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:-all}"

cd "$root"

run_kafka() {
  docker compose up -d --wait kafka
  for svc in event-service analytics-service; do
    (cd "services/$svc" && go test -tags integration -count=1 -timeout 10m ./internal/adapter/messaging/kafka/)
  done
  (cd services/booking-service && cargo test --test kafka_integration -- --ignored --test-threads=5)
}

run_http() {
  trap 'docker compose --profile gateway-test rm -sfv wiremock >/dev/null' RETURN
  docker compose --profile gateway-test up -d --wait wiremock
  (cd services/user-service && cargo test --test gateway_integration -- --ignored)
}

case "$target" in
  kafka) run_kafka ;;
  http) run_http ;;
  all)
    run_kafka
    run_http
    ;;
  *)
    echo "usage: $0 [kafka|http|all]" >&2
    exit 2
    ;;
esac
