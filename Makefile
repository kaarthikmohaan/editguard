.DEFAULT_GOAL := help
.PHONY: help setup up down test lint

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
