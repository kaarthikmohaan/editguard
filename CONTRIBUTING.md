# Contributing

## Setup

Requirements: macOS on Apple Silicon, Docker Desktop (10 to 12 GB memory), Homebrew, uv, an AWS account with SSO.

```bash
brew install openjdk@17 terraform awscli gitleaks
git clone https://github.com/<you>/editguard && cd editguard
cp .env.example .env
security add-generic-password -a editguard -s anthropic -w '<key>'   # stored in Keychain
aws sso login --profile editguard-dev
make setup        # uv sync --all-groups; pre-commit install
make up           # local services
make test         # unit tests
```

Python is pinned to 3.12 in `.python-version`. Add packages with `uv add <pkg>` (runtime), `uv add --group transform <pkg>` (dbt), or `uv add --dev <pkg>`. Airflow runs in its own container.

## Workflow

- Trunk-based: branch from `main`, keep branches under a day, open a pull request, and merge with **Create a merge commit** when CI is green. Merge commits keep each reviewed Conventional Commit on `main` with its original hash, so `git log` and `git bisect` show the real steps, and a local `main` then updates with `git pull --ff-only`.
- `main` is protected by a ruleset: no direct pushes, force pushes or deletion; the 7 CI checks must pass and review conversations must be resolved before merging. Tags are not affected.
- CI (`.github/workflows/ci.yml`) runs on every pull request and push to `main`: lint and gitleaks, unit tests with coverage, `make contract-check`, dbt on DuckDB, the Spark and integration tests, and pip-audit. No AWS is involved. Dependabot opens weekly update pull requests (`.github/dependabot.yml`).
- One GitHub issue per task; link it in the PR.
- Commits follow Conventional Commits: `feat:`, `fix:`, `test:`, `docs:`, `refactor:`, `chore:`.
- Any change to a design decision needs a new ADR in `docs/adr/`.
- Tests go in the same PR as the code. Docs (README, runbook, dictionary) go in the same PR as the change.

## Releases

1. A pull request bumps `version` in `pyproject.toml` (then `uv lock`) and turns `[Unreleased]` in `CHANGELOG.md` into the new version; merge it.
2. On the updated `main`: `git tag -a vX.Y.Z -m "..."` and `git push origin vX.Y.Z`. The tag must equal `v` + the project version.
3. `.github/workflows/release.yml` builds and pushes the images, checks staging for Terraform drift (ADR 0012), runs the replay smoke test on staging with those images, then waits for a reviewer to approve the `prod` environment.
4. After approving: `git switch --detach vX.Y.Z` and `make deploy VERSION=vX.Y.Z` (runbook section 2); then `git switch main`.

Never move a published tag: if a release fails after its tag is pushed, fix it and release a new patch version.

## Coding standards

- `ruff` is the style guide: rule sets E, F, I, B, UP, S; line length 100. `make lint` must pass.
- Type hints on public functions; docstrings on public functions and modules only.
- No secrets, usernames or diff text in logs. Use `structlog` with `service`, `event_id`, `rev_id`, `wiki_id`, `score_version`.
- Pure functions for parsing, features and labels, so they are unit-testable without Spark.
- Data shapes come from `contracts/edits.odcs.yaml`; regenerate with `make contract` instead of editing generated files.

## Contract changes

1. Edit `contracts/edits.odcs.yaml` and bump its `version`.
2. `make contract` regenerates the Avro, Pydantic and dbt schema files in `contracts/generated/` and the contract tables in `docs/data-dictionary.md` (datacontract-cli, pinned, run through `uvx`). Commit them with the contract. `make contract-check` (run in CI) fails if any of them is stale.
3. CI checks BACKWARD compatibility against Schema Registry; breaking changes need an ADR and a new topic version (`.v2`).

## Make targets

| Target | Does |
| --- | --- |
| `setup` | Install dependencies and hooks |
| `infra ENV=` | Terraform plan and apply for staging or prod (dev is local, no AWS) |
| `demo` | Replay a fixture and open the triage page |
| `up` / `down` | Start / stop local services |
| `stream` / `stream-down` / `images` | Start / stop the producers and live job in Docker / build their images locally |
| `replay` / `batch` | Run a replay, Airflow |
| `test` / `test-all` / `coverage` / `e2e` / `perf-stream` / `perf-api` / `evals` | Test layers (`test` is unit only) |
| `lint` | ruff, gitleaks |
| `contract` / `contract-check` | Regenerate schemas and dictionary tables / check they are current (with `datacontract lint`) |
| `report` | Evaluation and cost reports |
| `window-start` / `window-end` | Mark SLO run windows |
| `purge` | Remove suppressed revisions from bronze |
| `restore-pg DATE=` / `replay-from-bronze` | Restore drills |
| `chaos-*` | Failure drills |
| `deploy VERSION=` | Deploy an approved release to prod: images pinned by digest, prod Terraform and dbt, restart streaming |
| `teardown` | Destroy cloud resources |
