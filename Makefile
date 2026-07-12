.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Sets up the dev environment (uv sync) and installs pre-commit hooks.
	@\
	uv sync && uv run pre-commit install

.PHONY: mypy
mypy: ## Runs mypy for static type checking.
	@\
	uv run mypy

.PHONY: ruff
ruff: ## Runs ruff lint checks.
	@\
	uv run ruff check .

.PHONY: format
format: ## Runs ruff format checks.
	@\
	uv run ruff format --check .

.PHONY: lint
lint: ## Runs ruff and mypy code checks.
	@\
	uv run ruff check .; \
	uv run ruff format --check .; \
	uv run mypy

.PHONY: all
all: ## Runs all pre-commit checks against staged changes.
	@\
	uv run pre-commit run -a

.PHONY: linecheck
linecheck: ## Checks for all Python lines 100 characters or more
	@\
	find dbt -type f -name "*.py" -exec grep -I -r -n '.\{100\}' {} \;

.PHONY: unit
unit: ## Runs unit tests.
	@\
	uv run pytest -n auto -ra -v tests/unit

.PHONY: functional
functional: ## Runs functional tests.
	@\
	uv run pytest -n auto -ra -v tests/functional

.PHONY: test
test: ## Runs unit tests and code checks.
	@\
	uv run pytest -n auto -ra -v tests/unit; \
	uv run ruff check .; \
	uv run ruff format --check .; \
	uv run mypy

.PHONY: server
server: ## Spins up a local MS SQL Server instance for development. Docker-compose is required.
	@\
	docker compose up -d

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
