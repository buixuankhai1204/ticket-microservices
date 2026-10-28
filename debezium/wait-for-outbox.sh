#!/bin/sh
set -eu

timeout="${OUTBOX_READY_TIMEOUT:-300}"
deadline=$(( $(date +%s) + timeout ))

for entry in $OUTBOX_DATABASES; do
  name="${entry%%=*}"
  url="${entry#*=}"
  until [ "$(psql "$url" -Atc "select to_regclass('public.outbox_events') is not null" 2>/dev/null || true)" = "t" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timed out after ${timeout}s waiting for outbox_events in $name" >&2
      exit 1
    fi
    sleep 1
  done
  echo "outbox_events exists in $name"
done
