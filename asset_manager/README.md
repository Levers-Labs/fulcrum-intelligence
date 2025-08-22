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
# Install dependencies (includes commons and query_manager)
make install
```

### Environment Configuration

Create a `.env` file with required settings:

```bash
# Service endpoints
APP_STORY_MANAGER_SERVER_HOST=http://localhost:8002
APP_ANALYSIS_MANAGER_SERVER_HOST=http://localhost:8000
APP_QUERY_MANAGER_SERVER_HOST=http://localhost:8001
APP_INSIGHTS_BACKEND_SERVER_HOST=http://localhost:8004

# Auth0 configuration
APP_AUTH0_API_AUDIENCE=your-audience
APP_AUTH0_ISSUER=https://your-domain.auth0.com/
APP_AUTH0_CLIENT_ID=your-client-id
APP_AUTH0_CLIENT_SECRET=your-client-secret

# Application settings
APP_SECRET_KEY=your-secret-key
APP_ENV=dev
APP_SENDER_EMAIL="Levers Insights <notifications@leverslabs.com>"
APP_AWS_REGION=us-west-1
```

## Development

### Start Development Server
```bash
# Start Dagster dev server with UI (recommended)
make dev
```

The UI will be available at http://localhost:3000

### Alternative Commands
```bash
# Start UI only
make ui

# Start daemon for schedules/sensors
make daemon

# Materialize all assets
make materialize

# Materialize specific asset with partition
make materialize-asset ASSET=metric_semantic_values

# Run specific job
make run-job JOB=full_pipeline

# List available jobs and schedules
make list-jobs
make list-schedules
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
   - Group: `semantic_loading`

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

```python
from dagster import DagsterInstance, materialize
from asset_manager.definitions import defs

# Materialize specific partition
instance = DagsterInstance.get()
result = materialize(
    [defs.get_asset_def("metric_semantic_values")],
    partition_key={"tenant": "123", "metric": "revenue", "grain": "day"},
    instance=instance
)
```

## Project Structure

```
asset_manager/
├── asset_manager/
│   ├── definitions.py           # Main Dagster definitions
│   ├── partitions.py           # Dynamic partition definitions
│   ├── assets/
│   │   └── snowflake_cache.py # Semantic extraction & loading
│   ├── resources/
│   │   ├── config.py           # App configuration from env
│   │   └── snowflake.py        # Snowflake client resource
│   ├── services/
│   │   ├── auth.py             # Auth utilities
│   │   └── snowflake_sync_service.py # Core sync logic
│   ├── jobs/
│   │   └── cache_job.py        # Pipeline job definition
│   ├── schedules/
│   │   └── daily_cache_schedule.py # 3 AM daily schedule
│   └── sensors/
│       └── partition_sync_sensor.py # Dynamic partition sync
├── run.py                      # Programmatic execution
└── Makefile                   # Development commands
```

## Monitoring & Observability

### Asset Metadata
- **Row counts** and **date windows** for extractions
- **Target tables** and **load statistics** for Snowflake
- **Sync types** (full vs incremental) per partition
- **Preview data** for debugging

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
# Check partition status
dagster asset materialize --select "metric_semantic_values" --partition '{"tenant": "123", "metric": "revenue", "grain": "day"}'

# View sensor evaluation
dagster sensor list
dagster sensor logs partition_sync_sensor

# Test schedule logic
dagster schedule preview daily_3am_schedule
```

## Migration from Prefect

This service replaces the `snowflake_cache_sync_for_tenant` Prefect flow with:

- **Better observability** through Dagster UI
- **Declarative partitioning** instead of dynamic task generation
- **Resource management** with dependency injection
- **Built-in retry logic** and **failure handling**
- **Asset-based thinking** vs task-based workflows