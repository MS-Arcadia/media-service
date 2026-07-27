# Arcadia media service.
#
#   make install   create .venv and install everything
#   make test      run the tests
#   make lint      ruff check plus format check
#   make docker    build the image
#
# The image is built here, not in the infra repository: how a service is built is that
# service's business.

SERVICE  := media-service
IMAGE    := arcadia/$(SERVICE)
VERSION  ?= local
VENV     := .venv
PY       := $(VENV)/bin/python

.DEFAULT_GOAL := help
.PHONY: help install test cover lint fmt run docker clean

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## Create the virtualenv and install dependencies
	python3 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e '.[dev]'
	@echo "installed"

test: ## Run the tests (no database or broker needed)
	$(PY) -m pytest -q

cover: ## Run the tests with a coverage report
	$(PY) -m pytest --cov=app --cov-report=term-missing

lint: ## ruff check plus format check
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

fmt: ## Format the code
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

run: ## Run locally against the infra stack
	$(VENV)/bin/uvicorn app.main:app --reload --port 8084

docker: ## Build the image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .
	@echo "built $(IMAGE):$(VERSION)"

clean: ## Remove the virtualenv and caches
	rm -rf $(VENV) .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
