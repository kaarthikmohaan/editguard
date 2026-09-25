# CLAUDE.md

- `docs/design.md` and the ADRs in `docs/adr/` are the source of truth. The design is frozen (2026-09-25).
- Any change to a design decision needs a new ADR (copy `docs/adr/0000-template.md`) before code changes. Never drift from the design silently; if a request conflicts with it, say so.
- Do not build anything from the "v2" or "Out of scope" lists in `docs/design.md` §3.
- Work milestone by milestone (M0 → M8); finish and tag one before starting the next.
- The owner is a junior data engineer building this to learn: explain steps, keep changes small, let them run commands and commit.
- Conventional Commits, tests and docs in the same change, no secrets in git. Ask before anything that costs money (AWS, Claude API), deletes data, or pushes to GitHub.
