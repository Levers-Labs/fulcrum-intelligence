.PHONY: help setup up down logs build clean status health test-services
DAGSTER_HOME:=$(shell pwd)/.dagster

# Default target
help:
	@echo ""
	@echo "🐳 Asset Manager Docker Compose Commands"
	@echo "======================================="
	@echo ""
	@echo "Setup Commands:"
	@echo "  setup              - Copy env template and instructions"
	@echo "  build              - Build Docker images"
	@echo ""
	@echo "Service Commands:"
	@echo "  dev                - Start all services in development mode"
	@echo "  up                 - Start all services"
	@echo "  down               - Stop all services"
	@echo "  restart            - Restart all services"
	@echo "  status             - Show service status"
	@echo ""
	@echo "Development Commands:"
	@echo "  logs               - View all service logs"
	@echo "  logs-web           - View web server logs only"
	@echo "  logs-daemon        - View daemon logs only"
	@echo "  logs-db            - View database logs only"
	@echo ""
	@echo "Maintenance Commands:"
	@echo "  health             - Check service health"
	@echo "  test-services      - Test connection to Fulcrum services"
	@echo "  clean              - Remove all containers and volumes"
	@echo "  reset              - Clean and restart everything"
	@echo ""
	@echo "Database Commands:"
	@echo "  db-shell           - Connect to PostgreSQL shell"
	@echo "  db-backup          - Backup Dagster database"
	@echo ""
	@echo "🌐 Access Points:"
	@echo "  Dagster UI:        http://localhost:3000"
	@echo "  PostgreSQL:        localhost:5439 (dagster/dagster_password)"
	@echo ""

# Setup environment
setup:
	@echo "📋 Environment setup..."
	@if [ -f .env ]; then \
		echo "✅ Using existing .env file"; \
		echo "🔧 Next steps:"; \
		echo "1. Ensure Fulcrum services are running locally"; \
		echo "2. Verify Auth0 credentials in .env"; \
		echo "3. Run 'make up' to start services"; \
		echo ""; \
	else \
		echo "❌ .env file not found"; \
		echo "Please create .env file with required configuration"; \
	fi

dev:
	@echo "🚀 Starting Asset Manager services in development mode..."
	@if [ ! -d $(DAGSTER_HOME) ]; then \
		mkdir -p $(DAGSTER_HOME); \
	fi
	DAGSTER_HOME=$(DAGSTER_HOME) dagster dev --host 0.0.0.0 --port 3000 -w workspace.yaml

# Build images
build:
	@echo "🏗️  Building Docker images..."
	docker-compose build

# Start services
up:
	@echo "🚀 Starting Asset Manager services..."
	docker-compose up -d
	@echo "✅ Services started"
	@echo "🌐 Dagster UI: http://localhost:3000"

# Stop services
down:
	@echo "🛑 Stopping Asset Manager services..."
	docker-compose down
	@echo "✅ Services stopped"

# Restart services
restart: down up

# Show service status
status:
	@echo "📊 Service Status:"
	docker-compose ps

# View logs
logs:
	@echo "📋 Following all service logs (Ctrl+C to exit)..."
	docker-compose logs -f

logs-web:
	@echo "📋 Following web server logs (Ctrl+C to exit)..."
	docker-compose logs -f asset-manager-web

logs-daemon:
	@echo "📋 Following daemon logs (Ctrl+C to exit)..."
	docker-compose logs -f asset-manager-daemon

logs-db:
	@echo "📋 Following database logs (Ctrl+C to exit)..."
	docker-compose logs -f postgres

# Check service health
health:
	@echo "🏥 Checking service health..."
	@echo ""
	@echo "PostgreSQL:"
	@docker-compose exec postgres pg_isready -U dagster -d dagster || echo "❌ Database not ready"
	@echo ""
	@echo "Dagster Web Server:"
	@curl -s -f http://localhost:3000/server_info > /dev/null && echo "✅ Web server healthy" || echo "❌ Web server not responding"
	@echo ""
	@echo "Docker containers:"
	@docker-compose ps

# Test connections to Fulcrum services
test-services:
	@echo "🔍 Testing connections to Fulcrum services..."
	@echo ""
	@echo "Analysis Manager (port 8000):"
	@curl -s -f http://localhost:8000/health > /dev/null && echo "✅ Connected" || echo "❌ Not available"
	@echo "Query Manager (port 8001):"
	@curl -s -f http://localhost:8001/health > /dev/null && echo "✅ Connected" || echo "❌ Not available"
	@echo "Story Manager (port 8002):"
	@curl -s -f http://localhost:8002/health > /dev/null && echo "✅ Connected" || echo "❌ Not available"
	@echo "Insights Backend (port 8004):"
	@curl -s -f http://localhost:8004/health > /dev/null && echo "✅ Connected" || echo "❌ Not available"

# Database shell
db-shell:
	@echo "🐘 Connecting to PostgreSQL shell..."
	docker-compose exec postgres psql -U dagster -d dagster

# Database backup
db-backup:
	@echo "💾 Backing up Dagster database..."
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S) && \
	docker-compose exec postgres pg_dump -U dagster -d dagster > backups/dagster_backup_$$TIMESTAMP.sql && \
	echo "✅ Backup saved to backups/dagster_backup_$$TIMESTAMP.sql"

# Clean everything
clean:
	@echo "🧹 Cleaning up all containers and volumes..."
	docker-compose down -v --remove-orphans
	@echo "🧹 Removing unused images..."
	docker image prune -f
	@echo "✅ Cleanup complete"

# Reset everything
reset: clean build up
	@echo "🔄 Reset complete"

# Development helpers
shell-web:
	@echo "🐚 Opening shell in web container..."
	docker-compose exec asset-manager-web bash

shell-daemon:
	@echo "🐚 Opening shell in daemon container..."
	docker-compose exec asset-manager-daemon bash

# Check environment file
check-env:
	@echo "🔍 Checking environment configuration..."
	@if [ -f .env ]; then \
		echo "✅ .env exists"; \
		echo "📋 Environment variables:"; \
		grep -E '^[A-Z]' .env | head -10; \
		echo "..."; \
	else \
		echo "❌ .env not found"; \
		echo "Please create .env file with required configuration"; \
	fi
