# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Core Commands

### Development Setup
- `make setup` - Full project setup including pyenv, venv, Poetry, and dependencies
- `make setup-pre-commit` - Install pre-commit hooks
- `make install-all-deps` - Install dependencies for all modules

### Running Services
- `make run app=<service> port=<port>` - Run individual service (e.g., `make run app=query_manager port=8001`)
- `make run-all` - Run all services simultaneously (ports: query_manager:8001, analysis_manager:8000, story_manager:8002, insights_backend:8004)
- `python manage.py --help` - View CLI commands

### Code Quality
- `make format path=<path>` or `make format-all` - Format code with Black and isort
- `make lint path=<path>` or `make lint-all` - Lint with Ruff and MyPy
- `python manage.py format <paths>` - Format specific paths
- `python manage.py lint <paths> [--fix] [--raise-error]` - Lint specific paths

### Testing
- `make test app=<service>` - Run tests for specific service
- `make test-all` - Run tests for all services
- Individual service: `cd <service> && poetry run pytest --cov-report term-missing`

### Database Management
- Each service has its own `manage.py` with database commands
- `cd <service> && python manage.py --help` for service-specific commands

## Architecture Overview

This is a microservices-based analytics platform with the following structure:

### Core Components
- **commons/** - Shared utilities, auth, database models, and client libraries
- **core/fulcrum_core/** - Core analytics engine and modules (forecasting, drift detection, etc.)
- **levers/** - Analytics primitives and patterns for metric analysis

### Services
- **query_manager/** - Data querying and semantic model management (port 8001)
- **analysis_manager/** - Pattern analysis and component drift detection (port 8000)
- **story_manager/** - Story generation and narrative building (port 8002)
- **insights_backend/** - User management, notifications, and tenant operations (port 8004)
- **tasks_manager/** - Prefect-based workflow orchestration

### Database Schemas
Each service uses its own PostgreSQL schema:
- `analysis_store` - Analysis Manager data
- `query_store` - Query Manager metadata
- `story_store` - Story Manager content
- `insights_store` - Insights Backend user/tenant data

### Key Patterns
- Each service follows FastAPI structure with `main.py`, `config.py`, and organized modules
- Shared code in `commons/` for auth, database operations, and HTTP clients
- Poetry for dependency management in each module
- Alembic migrations in each service
- Multi-tenant architecture with tenant-specific database access

### Development Notes
- Python 3.10+ required
- Uses Poetry for dependency management
- Pre-commit hooks for code quality (Black, isort, Ruff, MyPy)
- All services can run independently or together
- Environment variables required in each service's `.env` file
- Auth0 integration for authentication across services

### Environment Setup
Services require `.env` files with AUTH0 credentials and database URLs. See README.md for complete environment variable setup.
