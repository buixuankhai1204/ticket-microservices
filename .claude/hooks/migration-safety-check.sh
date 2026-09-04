#!/bin/bash
# PostToolUse hook: after Claude writes/edits a file under a service's migrations/
# directory, scans the just-written .sql for DDL that is unsafe during a rolling
# deploy (the running service version keeps serving while the new schema applies)
# or that breaks this repo's transaction-wrapped migration runner. Runs after the
# write already happened (PostToolUse can't block it) — exit 2 surfaces the issue
# to Claude immediately so it's fixed in the same turn instead of surviving to a
# migration-reviewer pass or a production incident.
#
# See CLAUDE.md ("zero-downtime migrations") and the /new-migration skill for the
# full expand/contract rules; this enforces the grep-obvious subset.
set -euo pipefail

INPUT=$(cat)

python3 - "$INPUT" <<'PY'
import json, os, re, subprocess, sys

try:
    data = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)

if data.get("tool_name") not in ("Write", "Edit"):
    sys.exit(0)

path = (data.get("tool_input") or {}).get("file_path", "")
if not path or not os.path.isfile(path):
    sys.exit(0)
if not re.search(r"/services/[^/]+/migrations/[^/]+\.sql$", path):
    sys.exit(0)

src = open(path, "r", encoding="utf-8", errors="replace").read()
first_line = src.splitlines()[0] if src else ""
no_txn = "+migrate NoTransaction" in first_line

# Drop -- line comments, collapse whitespace, split into statements.
code = re.sub(r"--[^\n]*", " ", src)
code = re.sub(r"\s+", " ", code).strip()
stmts = [s.strip() for s in code.split(";") if s.strip()]

violations = []
def add(v): violations.append(v)

for s in stmts:
    u = s.upper()

    if re.search(r"ADD\s+COLUMN\b", u) and re.search(r"\bNOT\s+NULL\b", u) and "DEFAULT" not in u:
        add("ADD COLUMN ... NOT NULL with no DEFAULT: rewrites every row under a strong "
            "lock and breaks the still-running old version's inserts. Add a constant "
            "DEFAULT, or split expand (nullable) -> backfill -> SET NOT NULL via a NOT "
            "VALID check.")

    if re.search(r"\bDROP\s+COLUMN\b", u):
        add("DROP COLUMN: breaks the running old version immediately. Ship it only as the "
            "contract migration, a deploy after the code no longer uses that column.")
    if re.search(r"\bDROP\s+TABLE\b", u):
        add("DROP TABLE: contract phase only — a deploy after nothing reads/writes it.")
    if re.search(r"\bRENAME\s+COLUMN\b", u):
        add("RENAME COLUMN: the old version still queries the old name. Add new column, "
            "backfill, switch code, drop old — never rename in place.")
    if re.search(r"\bALTER\s+TABLE\b.*\bRENAME\s+TO\b", u):
        add("ALTER TABLE ... RENAME TO: breaks the running old version. Use a new table + "
            "backfill + code switch + drop.")
    if re.search(r"\bALTER\s+COLUMN\b.*\bTYPE\b", u):
        add("ALTER COLUMN ... TYPE: full table rewrite under lock unless a binary-"
            "compatible widen. Split via a new column + backfill, or note in the header "
            "why it's a no-op widen.")
    if re.search(r"\bALTER\s+COLUMN\b.*\bSET\s+NOT\s+NULL\b", u):
        add("SET NOT NULL: whole-table scan under lock. Add CHECK (col IS NOT NULL) NOT "
            "VALID, VALIDATE it in a later migration, then SET NOT NULL.")

    if re.search(r"\bADD\s+CONSTRAINT\b", u) and "NOT VALID" not in u:
        add("ADD CONSTRAINT without NOT VALID: whole-table scan holding a lock. Add it NOT "
            "VALID now, VALIDATE CONSTRAINT in a later migration.")

    if "CONCURRENTLY" in u and not no_txn:
        add("CONCURRENTLY inside a transaction-wrapped migration errors with 'cannot run "
            "inside a transaction block'. Put '-- +migrate NoTransaction' as the FIRST "
            "line and make the service's migrate runner run marked files outside a tx "
            "(see /new-migration step 5).")

# de-dupe, preserve order
seen, uniq = set(), []
for v in violations:
    if v not in seen:
        seen.add(v); uniq.append(v)

if uniq:
    try:
        root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"],
                                       text=True, stderr=subprocess.DEVNULL).strip()
        rel = os.path.relpath(path, root)
    except Exception:
        rel = path
    print(f"Migration safety issue in {rel}:", file=sys.stderr)
    for v in uniq:
        print(f"- {v}", file=sys.stderr)
    print("These are rolling-deploy hazards. See the /new-migration skill and CLAUDE.md's "
          "zero-downtime migrations note; migration-reviewer covers the rest.", file=sys.stderr)
    sys.exit(2)

sys.exit(0)
PY
