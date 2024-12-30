# Default target
.DEFAULT_GOAL := help

help:  ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies using Poetry
	poetry install

server:  ## Start Prefect server
	prefect server start

config:  ## Configure Prefect API URL
	prefect config set PREFECT_API_URL=http://localhost:4200/api

register-blocks:  ## Register Prefect blocks (requires server to be running)
	prefect blocks register --file tasks_manager/config.py

run:  ## Run the flow
	python run.py

run-flow:  ## Run a flow (usage: make run-flow GROUP=daily)
	prefect deployment run 'Generate Stories/story-generator' -p "group=$(GROUP)"

deploy-all:  ## Deploy all flows
	prefect deploy --all

clean:  ## Clean up generated files and virtual environment
	rm -f story-generator-deployment.yaml
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

setup: install config server
