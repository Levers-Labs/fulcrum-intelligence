.PHONY: help install format lint test setup server config create-pool worker deploy deploy-local run clean

# Default target
.DEFAULT_GOAL := help

help:  ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies using Poetry
	poetry install


# Prefect Commands with project isolation
setup:  ## Setup project-specific Prefect instance
	@echo "Setting up project-specific Prefect instance..."
	./scripts/setup_prefect.sh

server:  ## Start Prefect server with project configuration
	@echo "Starting Prefect server with project configuration..."
	@export $$(grep -v '^#' .prefectconfig | xargs) && prefect server start

config:  ## Configure Prefect for local development (automated via .prefectconfig)
	@echo "Prefect configuration is loaded automatically from .prefectconfig"
	@export $$(grep -v '^#' .prefectconfig | xargs) && prefect config view

create-pool:  ## Create local process work pool and queues
	@echo "Creating Prefect work pool and queues..."
	@export $$(grep -v '^#' .prefectconfig | xargs) && \
	prefect work-pool create --type process tasks-manager-local-pool

register-blocks:  ## Register Prefect blocks (requires server to be running)
	@export $$(grep -v '^#' .prefectconfig | xargs) && \
	prefect blocks register --file tasks_manager/config.py

worker:  ## Start a Prefect worker for local development
	@echo "Starting Prefect worker for local development..."
	@export $$(grep -v '^#' .prefectconfig | xargs) && \
	prefect worker start --pool tasks-manager-local-pool

deploy:  ## Deploy all flows to production (Prefect Cloud)
	prefect --no-prompt deploy --all

deploy-local:  ## Deploy all flows for local development
	@echo "Deploying all flows for local development..."
	@export $$(grep -v '^#' .prefectconfig | xargs) && \
	prefect --no-prompt deploy --all --prefect-file prefect.local.yaml

run-flow:  ## Run a specific deployment (usage: make run-flow DEPLOYMENT=generate-stories PARAMS='group="daily"')
	@export $$(grep -v '^#' .prefectconfig | xargs) && \
	prefect deployment run '$(DEPLOYMENT)' $(if $(PARAMS),--param $(PARAMS))

clean:  ## Clean up generated files and caches
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true

clean-prefect:  ## Clean Prefect data and reset local database
	@echo "Cleaning Prefect data..."
	rm -rf prefect_data/
	@echo "Prefect data cleaned. Run 'make setup' to reinitialize."

setup-local: install setup create-pool register-blocks deploy-local  ## Complete local development setup

setup-prod: install register-blocks deploy  ## Complete production deployment setup
