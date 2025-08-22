# Asset Manager

Dagster-based orchestration service for Fulcrum Intelligence Engine's Snowflake cache sync pipeline.

## Overview

This service migrates the Snowflake cache sync workflow from Prefect to Dagster, implementing:

- **Dynamic multi-dimensional partitions**: `tenant × metric × grain` combinations
- **Two-stage pipeline**: Semantic extraction → Snowflake loading
- **Tenant-aware partitioning**: Each tenant has specific metrics and grains
- **Automated scheduling**: Daily 3 AM runs for all active partitions
- **Real-time partition sync**: Sensor keeps partitions aligned with database config

## Pipeline Architecture

```
┌─────────────────────────┐    ┌──────────────────────────┐
│  metric_semantic_values │───▶│  snowflake_metric_cache  │
│                         │    │                          │
│ • Extract from Cube API │    │ • Load to Snowflake      │
│ • Per tenant/metric/    │    │ • Cache tables per       │
│   grain partition       │    │   tenant/metric/grain    │
└─────────────────────────┘    └──────────────────────────┘
```

## Setup

### Prerequisites
- Python 3.10+
- Poetry
- Access to Fulcrum services (Query Manager, Insights Backend, Snowflake)

### Installation

```bash
# Install dependencies and create Dagster home
make install

# Alternatively, using Poetry directly
poetry install
poetry run pip install -e .
mkdir -p .dagster  # Creates Dagster home directory
```

**Note**: The Dagster home directory (`.dagster/`) stores instance metadata, run history, and partition registries locally.

### Environment Configuration

Create a `.env` file with required settings:

```bash
# Service endpoints
STORY_MANAGER_SERVER_HOST=http://localhost:8002
ANALYSIS_MANAGER_SERVER_HOST=http://localhost:8000
QUERY_MANAGER_SERVER_HOST=http://localhost:8001
INSIGHTS_BACKEND_SERVER_HOST=http://localhost:8004
SERVER_HOST=http://localhost:3000

# Database configuration
DATABASE_URL=postgresql://user:password@localhost:5432/fulcrum_db

# Auth0 configuration
AUTH0_API_AUDIENCE=your-audience
AUTH0_ISSUER=https://your-domain.auth0.com/
AUTH0_CLIENT_ID=your-client-id
AUTH0_CLIENT_SECRET=your-client-secret

# Application settings
SECRET_KEY=your-secret-key
ENV=dev
SENDER_EMAIL="Levers Insights <notifications@leverslabs.com>"
AWS_REGION=us-west-1
```

## Development

### Start Development Server
```bash
# Start Dagster dev server with UI (recommended)
make dev

# Start UI only (if you have daemon running separately)
make ui

# Start daemon for schedules/sensors
make daemon
```

The UI will be available at http://localhost:3000

### Asset & Job Commands
```bash
# Materialize all assets
make materialize

# Materialize specific asset
make materialize-asset ASSET=metric_semantic_values

# Run specific job
make run-job JOB=snowflake_cache

# Run assets programmatically (with debugger support)
make run

# List available jobs and schedules
make list-jobs
make list-schedules

# Start/stop specific schedules
make start-schedule SCHEDULE=daily_snowflake_cache_schedule
make stop-schedule SCHEDULE=daily_snowflake_cache_schedule
```

### Cleanup Commands
```bash
# Clean Dagster storage only
make clean

# Clean everything (Dagster + Python cache)
make clean-all

# Show all available commands
make help
```

## Pipeline Components

### Assets

1. **`metric_semantic_values`**
   - Extracts metric time series from Query Manager
   - Partitioned by tenant/metric/grain
   - Includes metadata: date window, sync type, row counts
   - Group: `semantic_extraction`

2. **`snowflake_metric_cache`**
   - Loads extracted data into Snowflake cache tables
   - Uses `SnowflakeSemanticCacheManager` for tenant-aware loading
   - Emits load statistics and target table info
   - Group: `semantic_loader`

### Dynamic Partitions

- **Tenants**: Active tenants with `enable_metric_cache=true`
- **Metrics**: Per-tenant enabled metrics from cache config
- **Grains**: Per-tenant enabled grains (daily, weekly, monthly, etc.)

### Scheduling & Sensors

- **Daily Schedule**: Runs at 3 AM UTC for all active tenant/metric/grain combinations
- **Partition Sync Sensor**: Updates dynamic partitions every 5 minutes based on database config
- **Smart Scheduling**: Only schedules valid combinations per tenant

## Usage

### Via Dagster UI

1. Navigate to http://localhost:3000
2. View **Assets** tab to see pipeline lineage
3. **Materialize** individual partitions or full pipeline
4. Monitor runs in **Runs** tab
5. Check **Schedules** and **Sensors** tabs for automation status

### Programmatic Usage

#### Using Dagster API
```python
from dagster import DagsterInstance, MultiPartitionKey, materialize
from asset_manager.definitions import defs

# Materialize specific partition
instance = DagsterInstance.get()
partition_key = MultiPartitionKey({
    "tenant_metric": "123::revenue",  
    "tenant_grain": "123::day"
})
result = materialize(
    [defs.get_asset_def("metric_semantic_values")],
    partition_key=partition_key,
    instance=instance
)
```

#### Using CLI Script (Debugger-Friendly)
```bash
# Run specific asset with full debugging support
python run.py asset --asset metric_semantic_values --tenant 123 --metric revenue --grain day
python run.py asset --asset snowflake_metric_cache --tenant 123 --metric revenue --grain day

# Run complete job for a partition
python run.py job --job snowflake_cache --tenant 123 --metric revenue --grain day
```

The `run.py` script automatically handles:
- Dynamic partition registration in Dagster instance
- Resource configuration from environment
- Multi-partition key construction
- Direct asset materialization for IDE debugging

## Project Structure

```
asset_manager/
├── asset_manager/
│   ├── __init__.py
│   ├── definitions.py           # Main Dagster definitions
│   ├── partitions.py           # Dynamic partition definitions
│   ├── assets/
│   │   ├── __init__.py
│   │   └── snowflake_cache.py  # Semantic extraction & loading
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── config.py           # App configuration from env
│   │   ├── db.py               # Database session manager
│   │   └── snowflake.py        # Snowflake client resource
│   ├── services/
│   │   ├── __init__.py
│   │   ├── auth.py             # Auth utilities
│   │   ├── semantic_loader.py  # Metric data fetching & processing
│   │   ├── snowflake_sync_service.py # Core sync logic
│   │   └── utils.py            # Tenant/metric utilities
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── cache_job.py        # Pipeline job definition
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── daily_cache_schedule.py # 3 AM daily schedule
│   └── sensors/
│       ├── __init__.py
│       └── partition_sync_sensor.py # Dynamic partition sync
├── run.py                      # Programmatic execution (CLI + debugging)
├── Makefile                    # Development commands
└── pyproject.toml             # Poetry configuration
```

## Monitoring & Observability

### Asset Metadata

#### Semantic Extraction (`metric_semantic_values`)
- **Row counts** and **column information**
- **Date windows** (start/end dates) and **sync types** (full vs incremental)
- **Processing statistics** (total, processed, skipped records)
- **Data preview** (first 5 rows in markdown format)
- **Tenant/metric/grain** context for each partition

#### Snowflake Loading (`snowflake_metric_cache`)
- **Load timestamps** and **rows loaded**
- **Target table FQN** (fully qualified name)
- **Cache size** in MB and **load status**
- **Date window** and **sync type** information
- **Processing statistics** from upstream extraction

### Logging
- Structured logging with tenant/metric/grain context
- Integration with Dagster's built-in logging
- Service-level logs for auth, API calls, and database operations

### Dagster UI Features
- **Asset lineage** visualization
- **Partition status** tracking
- **Run history** and **failure analysis**
- **Resource utilization** monitoring

## Troubleshooting

### Common Issues

1. **Missing partitions**: Check sensor logs and database connectivity
2. **Auth failures**: Verify Auth0 configuration and service endpoints
3. **Snowflake connection**: Check Insights Backend Snowflake config
4. **Schedule not running**: Ensure daemon is running (`make daemon`)

### Debug Commands

```bash
# Check specific partition using CLI runner
python run.py asset --asset metric_semantic_values --tenant 123 --metric revenue --grain day

# Materialize specific partition using Dagster CLI
dagster asset materialize --select "metric_semantic_values" \
  --partition '{"tenant_metric": "123::revenue", "tenant_grain": "123::day"}' \
  -m asset_manager.definitions

# View sensor evaluation
dagster sensor list -m asset_manager.definitions
dagster sensor logs partition_sync_sensor -m asset_manager.definitions

# Test schedule logic
dagster schedule preview daily_snowflake_cache_schedule -m asset_manager.definitions

# List all available jobs
make list-jobs

# View asset status in UI
# Navigate to http://localhost:3000 after running `make dev`
```

## Migration from Prefect

This service replaces the `snowflake_cache_sync_for_tenant` Prefect flow with:

- **Better observability** through Dagster UI
- **Declarative partitioning** instead of dynamic task generation
- **Resource management** with dependency injection
- **Built-in retry logic** and **failure handling**
- **Asset-based thinking** vs task-based workflows