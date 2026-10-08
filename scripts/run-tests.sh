#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tier="${1:-unit}"
target="${2:-all}"

go_services=(analytics-service event-service)
rust_services=(booking-service user-service)
usage="usage: $0 [unit|integration|component|contract|e2e] [analytics|event|booking|user|all]"

run_go() {
  cd "$root/services/$1"
  case "$tier" in
    unit) go generate ./internal/usecase/ && go test ./... ;;
    integration) go test -tags integration -count=1 ./tests/ ;;
    component) go test -tags component -count=1 ./tests/ ;;
    contract) CGO_LDFLAGS="-L$HOME/.pact/lib" go test -tags contract -count=1 ./tests/ ;;
    e2e) go test -tags e2e -count=1 ./tests/ ;;
  esac
}

run_rust() {
  cd "$root/services/$1"
  case "$tier" in
    unit) cargo test ;;
    integration)
      if [[ "$1" == "booking-service" ]]; then
        cargo test --test integration_repository --test integration_kafka -- --ignored
      else
        cargo test --test integration_repository -- --ignored
        cargo test --test integration_gateways
      fi
      ;;
    component) cargo test --test component_in_process --test component_out_of_process -- --ignored ;;
    contract) cargo test --test contract_consumer ;;
    e2e) cargo test --test e2e -- --ignored ;;
  esac
}

case "$tier" in
  unit | integration | component | contract | e2e) ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac

if [[ "$tier" == "e2e" && -z "${E2E_BASE_URL:-}" ]]; then
  echo "set E2E_BASE_URL to the Kong gateway of a running stack, e.g. http://localhost:8000" >&2
  exit 2
fi

case "$target" in
  analytics) run_go analytics-service ;;
  event) run_go event-service ;;
  booking) run_rust booking-service ;;
  user) run_rust user-service ;;
  all)
    for svc in "${go_services[@]}"; do (run_go "$svc"); done
    for svc in "${rust_services[@]}"; do (run_rust "$svc"); done
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
