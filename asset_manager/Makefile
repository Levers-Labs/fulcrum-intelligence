.PHONY: install dev ui dev-file ui-file run materialize materialize-asset status clean clean-all help

# Set Dagster home directory
DAGSTER_HOME := $(shell pwd)/.dagster

# Install dependencies and package in development mode
install:
	poetry install
	poetry run pip install -e .
	mkdir -p $(DAGSTER_HOME)
	@echo "✅ Installed dependencies and created Dagster home at: $(DAGSTER_HOME)"

# Start development server with UI (recommended)
dev:
	@echo "🚀 Starting Dagster dev server..."
	@echo "📁 Using Dagster home: $(DAGSTER_HOME)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster dev -w workspace.yaml

# Start Dagster UI only
ui:
	@echo "🚀 Starting Dagster UI..."
	@echo "📁 Using Dagster home: $(DAGSTER_HOME)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster-webserver -w workspace.yaml

# Alternative module-based commands
dev-module:
	@echo "🚀 Starting Dagster dev server (module-based)..."
	@echo "📁 Using Dagster home: $(DAGSTER_HOME)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster dev -m asset_manager.definitions

ui-module:
	@echo "🚀 Starting Dagster UI (module-based)..."
	@echo "📁 Using Dagster home: $(DAGSTER_HOME)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster-webserver -m asset_manager.definitions

# Run assets programmatically
run:
	@echo "🔄 Running assets programmatically..."
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run python run.py

# Materialize all assets
materialize:
	@echo "⚡ Materializing all assets..."
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster asset materialize --select "*" -w workspace.yaml

# Materialize specific asset
materialize-asset:
	@echo "⚡ Materializing asset: $(ASSET)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster asset materialize --select "$(ASSET)" -w workspace.yaml

# Run specific job manually
run-job:
	@echo "🔄 Running job: $(JOB)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster job execute --job "$(JOB)" -w workspace.yaml

# List all jobs
list-jobs:
	@echo "📋 Available jobs:"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster job list -w workspace.yaml

# List all schedules
list-schedules:
	@echo "📅 Available schedules:"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster schedule list -w workspace.yaml

# Start/stop schedules (requires daemon)
start-schedule:
	@echo "▶️ Starting schedule: $(SCHEDULE)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster schedule start "$(SCHEDULE)" -w workspace.yaml

stop-schedule:
	@echo "⏹️ Stopping schedule: $(SCHEDULE)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster schedule stop "$(SCHEDULE)" -w workspace.yaml

# Start Dagster daemon (required for schedules/sensors)
daemon:
	@echo "🤖 Starting Dagster daemon for schedules and sensors..."
	@echo "📁 Using Dagster home: $(DAGSTER_HOME)"
	DAGSTER_HOME=$(DAGSTER_HOME) poetry run dagster-daemon run

# Clean Dagster storage only
clean:
	@echo "🧹 Cleaning Dagster storage..."
	rm -rf $(DAGSTER_HOME)
	@echo "✅ Cleaned Dagster storage"

# Clean everything (Dagster storage + Python cache)
clean-all:
	@echo "🧹 Cleaning everything..."
	rm -rf $(DAGSTER_HOME)
	rm -rf __pycache__/
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleaned everything"

# Show help
help:
	@echo ""
	@echo "🔧 Asset Manager - Dagster Commands"
	@echo "=================================="
	@echo ""
	@echo "Setup Commands:"
	@echo "  install              - Install dependencies and create Dagster home"
	@echo ""
	@echo "Development Commands:"
	@echo "  dev                 - Start development server with UI (recommended)"
	@echo "  ui                  - Start Dagster UI only"
	@echo "  dev-module          - Start dev server using module path"
	@echo "  ui-module           - Start UI using module path"
	@echo ""
	@echo "Asset Commands:"
	@echo "  run                 - Run assets programmatically"
	@echo "  materialize         - Materialize all assets"
	@echo "  materialize-asset   - Materialize specific asset (use ASSET=name)"
	@echo "  status              - Check asset status"
	@echo "  run-job             - Run specific job manually (use JOB=name)"
	@echo "  list-jobs           - List all available jobs"
	@echo "  list-schedules      - List all schedules"
	@echo "  start-schedule      - Start a schedule (use SCHEDULE=name)"
	@echo "  stop-schedule       - Stop a schedule (use SCHEDULE=name)"
	@echo "  daemon              - Start Dagster daemon (required for schedules)"
	@echo ""
	@echo "Cleanup Commands:"
	@echo "  clean               - Clean Dagster storage only"
	@echo "  clean-all           - Clean Dagster storage + Python cache"
	@echo ""
	@echo "Examples:"
	@echo "  make install                           # Setup everything"
	@echo "  make dev                              # Start development"
	@echo "  make run-job JOB=daily_sales_job          # Run specific job"
	@echo "  make start-schedule SCHEDULE=daily_sales_schedule  # Start schedule"
	@echo "  make daemon                           # Start daemon for schedules"
	@echo ""
	@echo "📁 Dagster home: $(DAGSTER_HOME)"
	@echo "🌐 UI will be available at: http://localhost:3000"
	@echo ""
