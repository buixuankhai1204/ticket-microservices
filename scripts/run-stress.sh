#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stress="$root/tests/stress"
generated="$stress/.generated"
overlay="$stress/docker-compose.stress.yml"
rate_limit="${STRESS_KONG_RATE_LIMIT:-100000000}"
k6_image="${STRESS_K6_IMAGE:-grafana/k6:0.55.0}"
kong_port="${KONG_PROXY_HOST_PORT:-8000}"
usage="usage: $0 [--direct] [--keep-rate-limits] <smoke|contention|soak> [-- <k6 args>]
       $0 <kong-lift|kong-restore>"

direct=0
keep_limits=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --direct) direct=1; shift ;;
    --keep-rate-limits) keep_limits=1; shift ;;
    -h | --help) echo "$usage"; exit 0 ;;
    *) break ;;
  esac
done

command="${1:-}"
if [[ -z "$command" ]]; then
  echo "$usage" >&2
  exit 2
fi
shift
lifted_here=0
k6_args=()
if [[ "${1:-}" == "--" ]]; then
  shift
  k6_args=("$@")
fi

compose() {
  docker compose -f "$root/docker-compose.yml" "$@"
}

kong_limit_header() {
  curl -sS -D - -o /dev/null "http://localhost:$kong_port/api/v1/events?limit=1" 2>/dev/null |
    tr -d '\r' | grep -i '^x-ratelimit-limit-minute:' || true
}

require_kong() {
  local running
  running="$(compose ps --services --status running)"
  if ! grep -qx kong <<<"$running"; then
    echo "kong is not running; start the stack first: docker compose up -d" >&2
    exit 1
  fi
}

wait_for_kong() {
  local want_lifted="$1" header
  for _ in $(seq 1 60); do
    header="$(kong_limit_header)"
    if [[ -n "$header" ]]; then
      if [[ "$want_lifted" == 1 && "$header" == *"$rate_limit"* ]]; then
        return 0
      fi
      if [[ "$want_lifted" == 0 && "$header" != *"$rate_limit"* ]]; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "kong did not reach the expected rate-limit state (last header: ${header:-none})" >&2
  return 1
}

lift_rate_limits() {
  require_kong
  mkdir -p "$generated"
  sed -E "s/^([[:space:]]*minute:[[:space:]]*)[0-9]+[[:space:]]*$/\\1${rate_limit}/" \
    "$root/kong/kong.yml" >"$generated/kong.yml"
  if ! grep -q -E "minute: ${rate_limit}\$" "$generated/kong.yml"; then
    echo "found no minute rate limits to lift in kong/kong.yml" >&2
    exit 1
  fi
  KONG_STRESS_CONFIG="$generated/kong.yml" compose -f "$overlay" up -d --no-deps --force-recreate kong
  lifted_here=1
  wait_for_kong 1
}

restore_rate_limits() {
  require_kong
  compose up -d --no-deps --force-recreate kong
  wait_for_kong 0
}

restore_on_exit() {
  if [[ "$lifted_here" == 1 ]]; then
    restore_rate_limits || echo "kong still has its rate limits lifted: run $0 kong-restore" >&2
  fi
}

run_k6() {
  local name="$1" script="$2" var
  mkdir -p "$generated"
  if command -v k6 >/dev/null 2>&1; then
    k6 run --summary-export "$generated/$name-summary.json" \
      ${k6_args[@]+"${k6_args[@]}"} "$stress/$script"
    return
  fi
  if ! command -v docker >/dev/null 2>&1; then
    echo "neither k6 nor docker is installed; install k6 (brew install k6)" >&2
    exit 1
  fi
  local env_args=()
  while IFS= read -r var; do
    env_args+=(-e "$var")
  done < <(env | sed -n 's/^\(STRESS_[A-Za-z0-9_]*\)=.*/\1/p')
  docker run --rm -i --user "$(id -u):$(id -g)" \
    --add-host host.docker.internal:host-gateway \
    -v "$stress:/stress" -w /stress \
    -e "STRESS_HOST=${STRESS_HOST:-host.docker.internal}" \
    ${env_args[@]+"${env_args[@]}"} \
    "$k6_image" run --summary-export "/stress/.generated/$name-summary.json" \
    ${k6_args[@]+"${k6_args[@]}"} "/stress/$script"
}

case "$command" in
  kong-lift) lift_rate_limits; exit 0 ;;
  kong-restore) restore_rate_limits; exit 0 ;;
  smoke) script=smoke.js ;;
  contention) script=seat-contention.js ;;
  soak) script=soak.js ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac

trap 'exit 130' INT TERM
trap restore_on_exit EXIT

if [[ "$direct" == 1 ]]; then
  export STRESS_TARGET=direct
elif [[ "$keep_limits" == 0 ]]; then
  lift_rate_limits
fi

run_k6 "$command" "$script"
