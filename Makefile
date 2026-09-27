.DEFAULT_GOAL := help
.PHONY: help setup up down test lint infra batch batch-down dbt replay replay-bronze replay-check

AWS_PROFILE ?= editguard-dev
export AWS_PROFILE
TF_ENV_DIR = infra/terraform/aws/env

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-14s %s\n", $$1, $$2}'

setup: ## Install dependencies and git hooks
	uv sync --all-groups
	uv run pre-commit install

up: ## Start local services and wait until healthy
	docker compose up -d --wait

down: ## Stop local services, Airflow included (keeps data volumes)
	docker compose --profile batch down

batch: ## Start Airflow (hourly dbt build, daily maintenance); UI at http://localhost:8080
	docker compose --profile batch up -d --build --wait airflow

batch-down: ## Stop Airflow only
	docker compose --profile batch stop airflow

test: ## Run unit tests
	uv run pytest

lint: ## ruff, format check, gitleaks on full history
	uv run ruff check .
	uv run ruff format --check .
	gitleaks git --no-banner --redact

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

replay-bronze: ## Append everything replayed so far to bronze.edits_replay: make replay-bronze ENV=…
	@test -n "$(ENV)" || { echo 'usage: make replay-bronze ENV=dev|staging|prod'; exit 1; }
	uv run python -m editguard.streaming.replay_job --env $(ENV)

replay-check: ## Replay-count check: make replay-check ENV=staging|prod REPORT=data/replays/….json
	@test -n "$(ENV)" -a -n "$(REPORT)" || { echo 'usage: make replay-check ENV=prod REPORT=data/replays/<file>.json'; exit 1; }
	args=$$(uv run python -m editguard.tools.replay_args $(REPORT)) && \
	  $(DBT) run-operation replay_count --args "$$args" --target $(ENV)
