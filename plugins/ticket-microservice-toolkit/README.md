# ticket-microservice-toolkit

Packages this repo's Clean Architecture + choreography-saga tooling (skills, review
subagents, enforcement hooks) as an installable Claude Code plugin, so the same conventions
can be reused in a sibling repo rather than copy-pasted.

## Assumes the target repo follows this project's conventions

The skills/hooks in here aren't fully generic — they assume the repo they're installed into
has:
- A `kong/kong.yml` (Kong declarative config `3.0`) that is the source of truth for service
  names, ports, and route prefixes.
- A `CLAUDE.md` documenting the Clean Architecture layering (`domain` / `usecase` /
  `adapter/http` / `adapter/repository` / `cmd`|`main`) and the Kafka choreography-saga
  conventions (transactional outbox, idempotent consumers).

If a target repo doesn't have those yet, copy the relevant sections from this repo's
`CLAUDE.md` first.

## What's included

- `skills/` — `new-go-service`, `new-rust-service`, `new-go-api-endpoint`,
  `new-rust-api-endpoint` (each covers a REST endpoint and/or a Kafka publish/consume saga
  step via `http:` / `publish:` / `consume:` args), `scalability-review`, `review-concurrency`
- `agents/` — `security-reviewer`, `api-contract-reviewer`
- `hooks/` — `pre-commit-check.sh` (blocks `git commit` on gofmt/vet/clippy failures, scoped
  to staged files' own service) and `clean-architecture-check.sh` (flags a dependency-rule
  violation right after a `domain`/`usecase`/`adapter` file is written)

## Test locally before publishing

```bash
claude --plugin-dir ./plugins/ticket-microservice-toolkit
```

## Why this isn't wired into `.claude/settings.json` here

This repo keeps its own `.claude/skills/`, `.claude/agents/`, and `.claude/hooks/` as the
zero-setup, always-on config for anyone who clones *this* repo directly — that's intentionally
left untouched. This `plugins/` directory is the same tooling repackaged for distribution to
*other* repos/teammates, not a replacement for the project-level config. The two currently
have duplicated content; if that drift becomes a problem, switch `.claude/` to reference this
plugin instead of shipping its own copies (see the inline-plugin-via-settings pattern in
Claude Code's plugin docs) — not done yet since it wasn't verified to auto-load without an
explicit `/plugin install` step.
