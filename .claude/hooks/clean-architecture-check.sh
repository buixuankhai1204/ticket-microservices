#!/bin/bash
# PostToolUse hook: after Claude writes/edits a file, checks the Clean Architecture
# dependency rule (see CLAUDE.md) for files under domain/, platform/port/, usecase/,
# adapter/http/, adapter/repository/, adapter/cache/, or adapter/messaging/ in any Go or
# Rust service. Runs after the write already happened
# (PostToolUse can't block it like PreToolUse can), so it can't undo the edit — but exit
# 2 surfaces the violation to Claude immediately, so it gets fixed in the same turn
# instead of surviving until a later /scalability-review or PR review.
set -euo pipefail

INPUT=$(cat)
TOOL_NAME=$(printf '%s' "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tool_name',''))" 2>/dev/null || echo "")
case "$TOOL_NAME" in
  Write|Edit) ;;
  *) exit 0 ;;
esac

FILE_PATH=$(printf '%s' "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tool_input',{}).get('file_path',''))" 2>/dev/null || echo "")
[ -n "$FILE_PATH" ] && [ -f "$FILE_PATH" ] || exit 0

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
[ -n "$REPO_ROOT" ] || exit 0
REL_PATH=${FILE_PATH#"$REPO_ROOT"/}

case "$REL_PATH" in
  */domain/*) LAYER=domain ;;
  */platform/port/*|*/platform/port.rs) LAYER=port ;;
  */usecase/*) LAYER=usecase ;;
  */adapter/http/*) LAYER=http ;;
  */adapter/repository/*) LAYER=repository ;;
  */adapter/cache/*) LAYER=cache ;;
  */adapter/messaging/*) LAYER=messaging ;;
  *) exit 0 ;;
esac

case "$FILE_PATH" in
  *.go) LANG=go ;;
  *.rs) LANG=rust ;;
  *) exit 0 ;;
esac

VIOLATIONS=""
add_violation() { VIOLATIONS="${VIOLATIONS}- ${1}\n"; }
hit() { grep -qE "$1" "$FILE_PATH" 2>/dev/null; }

if [ "$LANG" = "go" ]; then
  DRIVER_OR_FRAMEWORK='"net/http"|jackc/pgx|pgxpool|gin-gonic/gin|labstack/echo|gofiber/fiber|segmentio/kafka-go'
  # usecase/ owns the transaction boundary, so it is allowed to hold the pool and
  # name the tx handle (pgx/pgxpool) — but still no HTTP server / framework / Kafka client.
  USECASE_FORBIDDEN='"net/http"|gin-gonic/gin|labstack/echo|gofiber/fiber|segmentio/kafka-go'
  case "$LAYER" in
    domain)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "domain/ imports a framework or driver directly — domain must stay free of net/http, pgx/pgxpool, an HTTP framework, or a Kafka client. Move it behind a port interface instead."
      hit '"[^"]*/adapter/|"[^"]*/cmd/|"[^"]*/platform/port"' && add_violation "domain/ imports from adapter/, cmd/, or platform/port — dependencies must point inward only; domain cannot import outer layers."
      ;;
    port)
      hit '"[^"]*/adapter[/"]|"[^"]*/usecase[/"]|"[^"]*/cmd[/"]' && add_violation "platform/port/ imports from adapter/, usecase/, or cmd/ — a port interface may only depend on domain (plus the DB driver for the tx handle), never on outer layers."
      ;;
    usecase)
      hit "$USECASE_FORBIDDEN" && add_violation "usecase/ imports an HTTP server, HTTP framework, or Kafka client — usecase owns the DB transaction boundary (pgx/pgxpool is allowed) but transport belongs in adapter/."
      hit '"[^"]*/adapter/' && add_violation "usecase/ imports from adapter/ — usecase must receive dependencies via port interfaces injected at the composition root (cmd/main.go), not import adapter packages directly."
      ;;
    http)
      hit '"[^"]*/adapter/repository/' && add_violation "adapter/http/ imports adapter/repository/ directly — handlers must call through usecase, not bypass it to reach the repository."
      ;;
    repository)
      hit '"[^"]*/usecase/|"[^"]*/adapter/http/' && add_violation "adapter/repository/ imports usecase/ or adapter/http/ — repository adapters must only depend on domain (the interface they implement), never on outer layers."
      ;;
    cache)
      # The cache adapter implements a pure domain port (Cache) against Redis. It may
      # import the redis driver + domain; it must not reach into usecase or sibling adapters.
      hit '"[^"]*/usecase(/|")|"[^"]*/adapter/http(/|")|"[^"]*/adapter/repository(/|")' && add_violation "adapter/cache/ imports usecase or a sibling adapter — a cache adapter only implements a domain port (Cache), so it may depend on domain (and the redis driver), never on outer or sibling layers."
      ;;
    messaging)
      # The messaging adapter (Kafka consumer/DLQ) is an inbound adapter: it may hold the
      # Kafka client and call the usecase, like adapter/http does. It must not bypass the
      # usecase to touch the repository directly.
      hit '"[^"]*/adapter/repository(/|")' && add_violation "adapter/messaging/ imports adapter/repository/ directly — a consumer must drive its use case (which owns the transaction and the processed_events check), not call the repository itself."
      ;;
  esac
else
  DRIVER_OR_FRAMEWORK='use[[:space:]]+axum|use[[:space:]]+sqlx|use[[:space:]]+rdkafka|use[[:space:]]+tokio::net'
  # usecase/ owns the transaction boundary, so it is allowed to hold the pool and
  # name the connection handle (sqlx) — but still no axum / rdkafka / raw tokio::net.
  USECASE_FORBIDDEN='use[[:space:]]+axum|use[[:space:]]+rdkafka|use[[:space:]]+tokio::net'
  case "$LAYER" in
    domain)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "domain/ imports a framework or driver directly — domain must stay free of axum, sqlx, rdkafka, or raw tokio::net. Move it behind a port trait instead."
      hit 'use[[:space:]]+crate::adapter|use[[:space:]]+crate::main|use[[:space:]]+crate::platform::port' && add_violation "domain/ imports from crate::adapter, main, or platform::port — dependencies must point inward only; domain cannot import outer layers."
      ;;
    port)
      hit 'use[[:space:]]+crate::adapter|use[[:space:]]+crate::usecase' && add_violation "platform::port imports from crate::adapter or crate::usecase — a port trait may only depend on domain (plus the DB driver for the connection handle), never on outer layers."
      ;;
    usecase)
      hit "$USECASE_FORBIDDEN" && add_violation "usecase/ imports axum, rdkafka, or raw tokio::net — usecase owns the DB transaction boundary (sqlx is allowed) but transport belongs in adapter/."
      hit 'use[[:space:]]+crate::adapter' && add_violation "usecase/ imports crate::adapter — usecase must receive dependencies via port trait objects injected at the composition root (main.rs), not import adapter modules directly."
      ;;
    http)
      hit 'use[[:space:]]+crate::adapter::repository' && add_violation "adapter/http/ imports adapter::repository directly — handlers must call through usecase, not bypass it to reach the repository."
      ;;
    repository)
      hit 'use[[:space:]]+crate::usecase|use[[:space:]]+crate::adapter::http' && add_violation "adapter/repository/ imports usecase or adapter::http — repository adapters must only depend on domain (the trait they implement), never on outer layers."
      ;;
    cache)
      hit 'use[[:space:]]+crate::usecase|use[[:space:]]+crate::adapter::http|use[[:space:]]+crate::adapter::repository' && add_violation "adapter/cache/ imports usecase or a sibling adapter — a cache adapter only implements a domain port (Cache), so it may depend on crate::domain (and the redis driver), never on outer or sibling layers."
      ;;
    messaging)
      hit 'use[[:space:]]+crate::adapter::repository' && add_violation "adapter/messaging/ imports adapter::repository directly — a consumer must drive its use case (which owns the transaction and the processed_events check), not call the repository itself."
      ;;
  esac
fi

if [ -n "$VIOLATIONS" ]; then
  echo "Clean Architecture violation in $REL_PATH:" >&2
  printf '%b' "$VIOLATIONS" >&2
  echo "See CLAUDE.md's 'Application architecture: Clean Architecture per service' section." >&2
  exit 2
fi

exit 0
