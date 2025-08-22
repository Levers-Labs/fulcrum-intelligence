FROM python:3.11-slim

# Set working directory
RUN mkdir -p /opt/dagster/asset_manager/
WORKDIR /opt/dagster/asset_manager/

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONWARNINGS ignore::UserWarning
ENV PYTHONDONTWRITEBYTECODE 1

## Install System Deps
RUN apt-get update && apt-get -y install --no-install-recommends git gcc make libpq-dev python3-dev procps curl
RUN rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir --upgrade pip "poetry<2.0.0"

# Copy the shared dependencies first
COPY ./commons /opt/dagster/commons
COPY ./levers /opt/dagster/levers
COPY ./core /opt/dagster/core
COPY ./query_manager /opt/dagster/query_manager
COPY ./story_manager /opt/dagster/story_manager
COPY ./analysis_manager /opt/dagster/analysis_manager

# Install dependencies
COPY asset_manager/pyproject.toml ./asset_manager/poetry.lock* ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy application code
COPY asset_manager/ .

# Dagster instance home and config
ENV DAGSTER_HOME=/opt/dagster/asset_manager/.dagster
RUN mkdir -p $DAGSTER_HOME
COPY asset_manager/dagster-prod.yaml $DAGSTER_HOME/dagster.yaml

# Make monorepo modules importable
ENV PYTHONPATH=/opt/dagster

# Expose port
EXPOSE 3000

# run dagster webserver
CMD ["dagster-webserver", "-w", "workspace.yaml", "--port", "3000", "--host", "0.0.0.0"]
