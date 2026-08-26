#!/bin/bash
# PreToolUse hook: runs before Claude executes a Bash tool call. Only acts when the
# command is a `git commit`; every other Bash call passes straight through (exit 0).
# On a real commit attempt, lints/formats only the Go and Rust services whose files are
# actually staged, using each service's own toolchain if it's installed. Missing
# toolchains are skipped, not treated as failures — this hook enforces code quality on
# commits Claude makes in this session, it doesn't set up the machine.
set -euo pipefail

INPUT=$(cat)
TOOL_NAME=$(printf '%s' "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tool_name',''))" 2>/dev/null || echo "")
[ "$TOOL_NAME" = "Bash" ] || exit 0

COMMAND=$(printf '%s' "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tool_input',{}).get('command',''))" 2>/dev/null || echo "")
echo "$COMMAND" | grep -qE '(^|&&|;|\|)\s*git\s+commit\b' || exit 0

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

STAGED=$(git diff --cached --name-only --diff-filter=ACM || true)
[ -n "$STAGED" ] || exit 0

FAILED=0
FAIL_MSG=""

fail() {
  FAILED=1
  FAIL_MSG="${FAIL_MSG}${1}\n"
}

# --- Go: gofmt + go vet, scoped to each go.mod that owns a staged file ---
GO_FILES=$(echo "$STAGED" | grep '\.go$' || true)
if [ -n "$GO_FILES" ]; then
  if command -v go >/dev/null 2>&1; then
    GO_MODULE_DIRS=$(echo "$GO_FILES" | while read -r f; do
      dir=$(dirname "$f")
      while [ "$dir" != "." ] && [ ! -f "$dir/go.mod" ]; do dir=$(dirname "$dir"); done
      [ -f "$dir/go.mod" ] && echo "$dir"
    done | sort -u)
    for mod_dir in $GO_MODULE_DIRS; do
      UNFORMATTED=$(gofmt -l "$mod_dir" 2>/dev/null || true)
      [ -n "$UNFORMATTED" ] && fail "gofmt: unformatted files in $mod_dir:\n$UNFORMATTED\nRun: gofmt -w $mod_dir"
      VET_OUT=$(cd "$mod_dir" && go vet ./... 2>&1) || fail "go vet failed in $mod_dir:\n$VET_OUT"
    done
  else
    echo "note: 'go' not installed, skipping gofmt/go vet for staged Go files" >&2
  fi
fi

# --- Rust: cargo fmt --check + cargo clippy, scoped to each Cargo.toml that owns a staged file ---
RUST_FILES=$(echo "$STAGED" | grep -E '\.rs$|Cargo\.toml$' || true)
if [ -n "$RUST_FILES" ]; then
  if command -v cargo >/dev/null 2>&1; then
    CRATE_DIRS=$(echo "$RUST_FILES" | while read -r f; do
      dir=$(dirname "$f")
      while [ "$dir" != "." ] && [ ! -f "$dir/Cargo.toml" ]; do dir=$(dirname "$dir"); done
      [ -f "$dir/Cargo.toml" ] && echo "$dir"
    done | sort -u)
    for crate_dir in $CRATE_DIRS; do
      MANIFEST="$crate_dir/Cargo.toml"
      FMT_OUT=$(cargo fmt --manifest-path "$MANIFEST" --check 2>&1) || fail "cargo fmt --check failed in $crate_dir:\n$FMT_OUT\nRun: cargo fmt --manifest-path $MANIFEST"
      if cargo clippy --manifest-path "$MANIFEST" --version >/dev/null 2>&1; then
        CLIPPY_OUT=$(cargo clippy --manifest-path "$MANIFEST" -- -D warnings 2>&1) || fail "cargo clippy failed in $crate_dir:\n$CLIPPY_OUT"
      else
        echo "note: clippy component not installed, skipping clippy for $crate_dir (run: rustup component add clippy)" >&2
      fi
    done
  else
    echo "note: 'cargo' not installed, skipping fmt/clippy for staged Rust files" >&2
  fi
fi

if [ "$FAILED" -ne 0 ]; then
  printf '%b' "$FAIL_MSG" >&2
  echo "Commit blocked: fix the issues above, re-stage, and commit again." >&2
  exit 2
fi

exit 0
