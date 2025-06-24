#!/bin/bash

# Prefect Worker Environment Setup Script
# This script sets up the Python environment and dependencies for Prefect managed workers

set -e  # Exit on any error

echo "Setting up Prefect worker environment..."

# Install Poetry if not available
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry via pip..."
    pip install "poetry<2.0.0"
fi

echo "Poetry version:"
poetry --version

# Configure Poetry to not create virtual environments (use system Python)
poetry config virtualenvs.create false

# Install dependencies for tasks_manager only
echo "Installing tasks_manager dependencies..."
cd tasks_manager
poetry install --no-dev --no-interaction

echo "Environment setup complete!"

# Verify the installation by checking if key modules can be imported
echo "Verifying installation..."
python -c "
import sys
try:
    from commons import models
    print('✓ commons module imported successfully')
except ImportError as e:
    print(f'✗ Import error: {e}')
    sys.exit(1)
"

echo "Setup verification complete!"
