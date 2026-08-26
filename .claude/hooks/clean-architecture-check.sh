#!/bin/bash
# PostToolUse hook: after Claude writes/edits a file, checks the Clean Architecture
# dependency rule (see CLAUDE.md) for files under domain/, usecase/, adapter/http/, or
# adapter/repository/ in any Go or Rust service. Runs after the write already happened
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
  */usecase/*) LAYER=usecase ;;
  */adapter/http/*) LAYER=http ;;
  */adapter/repository/*) LAYER=repository ;;
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
  case "$LAYER" in
    domain)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "domain/ imports a framework or driver directly — domain must stay free of net/http, pgx/pgxpool, an HTTP framework, or a Kafka client. Move it behind a port interface instead."
      hit '"[^"]*/adapter/|"[^"]*/cmd/' && add_violation "domain/ imports from adapter/ or cmd/ — dependencies must point inward only; domain cannot import outer layers."
      ;;
    usecase)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "usecase/ imports a framework or driver directly — usecase must depend on domain ports only, not concrete pgx/http/Kafka libraries."
      hit '"[^"]*/adapter/' && add_violation "usecase/ imports from adapter/ — usecase must receive dependencies via domain interfaces injected at the composition root (cmd/main.go), not import adapter packages directly."
      ;;
    http)
      hit '"[^"]*/adapter/repository/' && add_violation "adapter/http/ imports adapter/repository/ directly — handlers must call through usecase, not bypass it to reach the repository."
      ;;
    repository)
      hit '"[^"]*/usecase/|"[^"]*/adapter/http/' && add_violation "adapter/repository/ imports usecase/ or adapter/http/ — repository adapters must only depend on domain (the interface they implement), never on outer layers."
      ;;
  esac
else
  DRIVER_OR_FRAMEWORK='use[[:space:]]+axum|use[[:space:]]+sqlx|use[[:space:]]+rdkafka|use[[:space:]]+tokio::net'
  case "$LAYER" in
    domain)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "domain/ imports a framework or driver directly — domain must stay free of axum, sqlx, rdkafka, or raw tokio::net. Move it behind a port trait instead."
      hit 'use[[:space:]]+crate::adapter|use[[:space:]]+crate::main' && add_violation "domain/ imports from crate::adapter or main — dependencies must point inward only; domain cannot import outer layers."
      ;;
    usecase)
      hit "$DRIVER_OR_FRAMEWORK" && add_violation "usecase/ imports a framework or driver directly — usecase must depend on domain port traits only, not concrete sqlx/axum/rdkafka types."
      hit 'use[[:space:]]+crate::adapter' && add_violation "usecase/ imports crate::adapter — usecase must receive dependencies via domain trait objects injected at the composition root (main.rs), not import adapter modules directly."
      ;;
    http)
      hit 'use[[:space:]]+crate::adapter::repository' && add_violation "adapter/http/ imports adapter::repository directly — handlers must call through usecase, not bypass it to reach the repository."
      ;;
    repository)
      hit 'use[[:space:]]+crate::usecase|use[[:space:]]+crate::adapter::http' && add_violation "adapter/repository/ imports usecase or adapter::http — repository adapters must only depend on domain (the trait they implement), never on outer layers."
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
