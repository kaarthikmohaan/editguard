.DEFAULT_GOAL := help
.PHONY: help setup up down test lint infra

AWS_PROFILE ?= editguard-dev
export AWS_PROFILE
TF_ENV_DIR = infra/terraform/aws/env

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-8s %s\n", $$1, $$2}'

setup: ## Install dependencies and git hooks
	uv sync --all-groups
	uv run pre-commit install

up: ## Start local services and wait until healthy
	docker compose up -d --wait

down: ## Stop local services (keeps data volumes)
	docker compose down

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
