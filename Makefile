.DEFAULT_GOAL := help
.PHONY: help setup up down stream stream-down images deploy release-tree report audit-sample audit-summary test test-all coverage dbt-ci lint contract contract-check dlq-replay format-check infra batch batch-down dbt replay replay-bronze replay-check

AWS_PROFILE ?= editguard-dev
export AWS_PROFILE
TF_ENV_DIR = infra/terraform/aws/env
# Airflow runs dbt and its DAGs from this checkout of the deployed release, never from the
# working folder, so a branch you are working on cannot reach prod (runbook: Airflow).
RELEASE_DIR = ../editguard-release

# After `make deploy`, .deploy.env pins prod and the release's images for every compose command.
ifneq ($(wildcard .deploy.env),)
include .deploy.env
export EDITGUARD_ENV EDITGUARD_PRODUCER_IMAGE EDITGUARD_SPARK_IMAGE
endif

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Install dependencies and git hooks
	uv sync --all-groups
	uv run pre-commit install

up: ## Start local services and wait until healthy
	docker compose up -d --wait

down: ## Stop local services, Airflow included (keeps data volumes)
	docker compose --profile batch down

stream: ## Start the producers and the Spark live job in Docker (released images after make deploy)
	docker compose --profile stream up -d --wait producer-edits producer-baseline live-job

stream-down: ## Stop the producers and the live job (they flush and save progress first)
	docker compose --profile stream stop producer-edits producer-baseline live-job

images: ## Build the producer and Spark images locally (used by make stream without .deploy.env)
	env -u EDITGUARD_PRODUCER_IMAGE -u EDITGUARD_SPARK_IMAGE \
	  docker compose --profile stream build producer-edits live-job

deploy: ## Deploy an approved release to prod: make deploy VERSION=vX.Y.Z (from that tag)
	@test -n "$(VERSION)" || { echo 'usage: make deploy VERSION=v0.2.0'; exit 1; }
	uv run python -m editguard.tools.deploy $(VERSION)
	$(MAKE) release-tree VERSION=$(VERSION)
	$(MAKE) infra ENV=prod
	$(MAKE) dbt ENV=prod CMD=build
	$(MAKE) stream

release-tree: ## Point Airflow at a release: make release-tree VERSION=vX.Y.Z (make deploy runs it)
	@test -n "$(VERSION)" || { echo 'usage: make release-tree VERSION=v0.2.1'; exit 1; }
	git fetch --tags --quiet
	if [ -d $(RELEASE_DIR) ]; then git -C $(RELEASE_DIR) switch --detach $(VERSION); \
	else git worktree add --detach $(RELEASE_DIR) $(VERSION); fi
	@echo "Airflow now runs $(VERSION) from $(RELEASE_DIR) (picked up at its next run)"

batch: ## Start Airflow (hourly dbt build, daily maintenance); UI at http://localhost:8080
	@test -d $(RELEASE_DIR) || { echo "no release checkout: make release-tree VERSION=<deployed tag>"; exit 1; }
	docker compose --profile batch up -d --build --wait airflow

batch-down: ## Stop Airflow only
	docker compose --profile batch stop airflow

test: ## Unit tests only (fast; no JVM, no network)
	uv run pytest -m unit

test-all: ## Every local layer: unit, dbt, Spark (Java 17) and integration (Docker)
	uv run pytest

dbt-ci: ## Every dbt model and data test on DuckDB from the 1,000-event fixture (no AWS)
	uv run --group transform python transform/ci/load_fixtures.py
	cd transform && DBT_PROFILES_DIR=. USERNAME_SALT=ci-salt DATA_BUCKET=unused \
	  uv run --group transform dbt build --target ci

coverage: ## Unit tests with a line and branch coverage report
	uv run pytest -m unit --cov --cov-report=term-missing:skip-covered

lint: ## ruff, format check, gitleaks on full history
	uv run ruff check .
	uv run ruff format --check .
	gitleaks git --no-banner --redact

format-check: ## Every Iceberg table in ENV is format v2 (ADR 0008): make format-check ENV=prod
	@test -n "$(ENV)" || { echo 'usage: make format-check ENV=staging|prod'; exit 1; }
	uv run --group transform python -m editguard.tools.format_check --env $(ENV)

contract: ## Regenerate Avro, Pydantic, dbt schema and dictionary tables from the contract
	scripts/contract/generate.sh

contract-check: ## Contract is valid ODCS and every generated file is up to date (CI)
	scripts/contract/check.sh

infra: ## Terraform plan + apply for one environment: make infra ENV=staging|prod
	@test -n "$(ENV)" || { echo "usage: make infra ENV=staging|prod"; exit 1; }
	terraform -chdir=$(TF_ENV_DIR) init -reconfigure -input=false \
	  -backend-config="bucket=editguard-tfstate-$$(aws sts get-caller-identity --query Account --output text)-ap-south-1" \
	  -backend-config="key=env/$(ENV)/terraform.tfstate"
	terraform -chdir=$(TF_ENV_DIR) apply -var-file=$(ENV).tfvars

DBT = cd transform && DBT_PROFILES_DIR=. \
  DATA_BUCKET=editguard-data-$$(aws sts get-caller-identity --query Account --output text)-ap-south-1 \
  USERNAME_SALT="$$(aws secretsmanager get-secret-value --secret-id editguard/username-salt \
    --query SecretString --output text)" \
  uv run --group transform dbt

dbt: ## Run dbt on Athena for one environment: make dbt ENV=staging|prod CMD="debug"
	@test -n "$(ENV)" -a -n "$(CMD)" || { echo 'usage: make dbt ENV=staging|prod CMD="debug"'; exit 1; }
	$(DBT) $(CMD) --target $(ENV)

replay: ## Replay a past window into edits.replay.v1: make replay SINCE=<iso> UNTIL=<iso>
	@test -n "$(SINCE)" -a -n "$(UNTIL)" || { echo 'usage: make replay SINCE=2026-09-26T10:00:00Z UNTIL=2026-09-26T11:00:00Z'; exit 1; }
	uv run python -m editguard.producer.replay --since $(SINCE) --until $(UNTIL)

dlq-replay: ## Re-send dead-lettered edits that now parse to edits.replay.v1 (then replay-bronze)
	uv run python -m editguard.tools.dlq_replay $(ARGS)

replay-bronze: ## Append everything replayed so far to bronze.edits_replay: make replay-bronze ENV=…
	@test -n "$(ENV)" || { echo 'usage: make replay-bronze ENV=dev|staging|prod'; exit 1; }
	uv run python -m editguard.streaming.replay_job --env $(ENV)

report: ## Evaluation report (ADR 0013): make report SNAPSHOT=latest|<id> [WINDOW=test] [SINCE= UNTIL=]
	@test -n "$(SNAPSHOT)" || { echo 'usage: make report SNAPSHOT=latest [WINDOW=test]'; exit 1; }
	uv run --group transform --group evaluation python -m editguard.tools.report \
	  --snapshot $(SNAPSHOT) --window $(or $(WINDOW),dev) \
	  $(if $(SINCE),--since $(SINCE)) $(if $(UNTIL),--until $(UNTIL))

audit-sample: ## Label-audit sheet from prod (read-only): make audit-sample KIND=logic|noise
	@test -n "$(KIND)" || { echo 'usage: make audit-sample KIND=logic|noise'; exit 1; }
	uv run --group transform --group evaluation python -m editguard.tools.audit sample --kind $(KIND)

audit-summary: ## Count the verdicts in the filled-in audit sheets under docs/audits/
	uv run --group evaluation python -m editguard.tools.audit summary

replay-check: ## Replay-count check: make replay-check ENV=staging|prod REPORT=data/replays/….json
	@test -n "$(ENV)" -a -n "$(REPORT)" || { echo 'usage: make replay-check ENV=prod REPORT=data/replays/<file>.json'; exit 1; }
	args=$$(uv run python -m editguard.tools.replay_args $(REPORT)) && \
	  $(DBT) run-operation replay_count --args "$$args" --target $(ENV)
