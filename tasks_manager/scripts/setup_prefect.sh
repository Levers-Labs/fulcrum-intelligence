#!/bin/bash

# Setup script for Tasks Manager Prefect instance
# This script initializes a project-specific Prefect server with local SQLite database

set -e

echo "Setting up Tasks Manager Prefect instance..."

# Create prefect data directory if it doesn't exist
mkdir -p prefect_data

# Export environment variables from .prefectconfig
echo "Loading Prefect configuration..."
export $(grep -v '^#' .prefectconfig | xargs)

# Initialize the database
echo "Initializing Prefect database..."
prefect server database reset --yes

echo "Tasks Manager Prefect setup complete!"
echo ""
echo "To start the Prefect server:"
echo "  make server"
echo ""
echo "To access the UI:"
echo "  http://localhost:4200"
echo ""
echo "Database location: ./prefect_data/prefect.db"
echo ""
echo "Next steps:"
echo "  1. make server       # Start Prefect server"
echo "  2. make create-pool  # Create work pool"
echo "  3. make register-blocks # Register configuration blocks"
echo "  4. make deploy-local # Deploy flows"
echo "  5. make worker       # Start worker"
