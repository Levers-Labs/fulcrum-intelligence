# Semantic Sync Migration to Dagster

## Overview

This document describes the migration of semantic sync functionality from Prefect (`tasks_manager/`) to Dagster (`asset_manager/`). The migration implements a scalable, partition-aware time series pipeline that supports efficient backfilling, automated scheduling, and dynamic partition management.

## Architecture

### Partition Strategy

**Time Partitions (Per Grain)**
- `daily_partitions`: Daily time windows (2025-07-01 onwards, format: "%Y-%m-%d")
- `weekly_partitions`: Weekly time windows (Monday-based, 2025-W27 onwards, format: "%Y-W%W")
- `monthly_partitions`: Monthly time windows (2025-07 onwards, format: "%Y-%m")

**Dynamic Partitions**
- `metric_contexts_partition`: Shared across all grains (`"tenant_identifier::metric_id"` format)
- `cache_tenant_grain_metric_partition`: Legacy cache partition (maintained for existing snowflake assets)

**Multi-Dimensional Partitions (2D Strategy)**
- `daily_time_series_partitions`: `date × metric_context`
- `weekly_time_series_partitions`: `week × metric_context`
- `monthly_time_series_partitions`: `month × metric_context`

**MetricContext Class**
- Pydantic model for consistent metric context parsing
- Methods: `from_string()`, `to_string()`, `key` property
- Format validation and error handling

### Services

**SemanticSyncService** (`services/semantic_sync.py`)
- Complete semantic sync orchestration with async operations
- Automatic tenant context management (`set_tenant_id`/`reset_context`)
- Intelligent sync type determination (FULL vs INCREMENTAL) per metric and dimension
- Dynamic sync window calculation based on grain and sync type
- Comprehensive error handling with detailed logging and recovery
- Database transaction management with proper cleanup
- Multi-dimensional data processing for all metric dimensions

**Core Service Methods:**
- `sync_metric_time_series()`: Main orchestration method
- `_determine_sync_type()`: Intelligent sync type selection
- `_calculate_sync_window()`: Date range calculation per grain
- `_fetch_and_store_metric_time_series()`: Base metric data handling
- `_fetch_and_store_dimensional_data()`: Dimensional data processing

**Pydantic Models**
- `SemanticSyncRequest`: Comprehensive input validation with computed properties
  - Fields: `tenant_id`, `tenant_identifier`, `metric_id`, `grain`, `sync_date`, `force_full_sync`
  - Properties: `metric_context` for partition key generation
- `SemanticSyncResponse`: Detailed sync results with computed metrics
  - Fields: All request data plus processing stats, timing, success status
  - Properties: `duration_seconds` for performance tracking

**Utility Functions** (`services/utils.py`)
- `get_tenant_identifier()`: Tenant identifier resolution via InsightBackendClient
- `get_tenant_id_by_identifier()`: Tenant ID lookup by identifier
- `get_metric()`: MetricDetail fetching from Query Manager DB
- `discover_metric_contexts()`: Dynamic partition discovery across all tenants/metrics

### Assets

**Time Series Assets** (`assets/time_series.py`)
- `metric_time_series_daily`: Daily grain sync with `daily_time_series_partitions`
- `metric_time_series_weekly`: Weekly grain sync with `weekly_time_series_partitions`
- `metric_time_series_monthly`: Monthly grain sync with `monthly_time_series_partitions`

**Asset Implementation Details:**
- **Group Name**: "time_series" for logical organization
- **Metadata**: Rich metadata including grain, type, owner for observability
- **Partition Parsing**: Extracts dimensions from multi-partition keys via `keys_by_dimension`
- **Multi-Partition Format**: Pipe-separated format like `"2025-07-01|tenant_a::cpu_usage"`
- **Date Handling**: Proper ISO format parsing and conversion for different grains
- **Service Integration**: Uses `SemanticSyncService` for all sync operations
- **Resource Dependencies**: Requires `AppConfigResource` and `DbResource`

**Key Features:**
- **Idempotent Execution**: Safe for backfills and re-runs
- **Comprehensive Logging**: Detailed start/completion logging with performance metrics
- **Rich Metadata**: `MaterializeResult` with complete response data as JSON metadata
- **Partition-Aware**: Handles multi-dimensional partition key extraction
- **Async Operations**: Full async/await pattern for performance
- **Error Propagation**: Detailed error messages and proper exception handling
- **Performance Tracking**: Records processing counts, dimensions, and duration

### Jobs

**Grain-Specific Jobs** (`jobs/time_series.py`)
- `daily_time_series_job`: Processes `metric_time_series_daily` with daily partitions
- `weekly_time_series_job`: Processes `metric_time_series_weekly` with weekly partitions
- `monthly_time_series_job`: Processes `metric_time_series_monthly` with monthly partitions
- `all_grains_time_series_job`: Combined job targeting entire "time_series" group for backfills

**Job Implementation Details:**
- **Asset Selection**: Uses `AssetSelection.assets()` for single assets, `AssetSelection.groups()` for combined jobs
- **Partitions Definition**: Each job inherits the appropriate time series partition definition
- **Rich Tagging**: Comprehensive tags including grain, type, owner for organization and filtering
- **Description**: Clear descriptions for operational visibility
- **Collections**: Organized in `grain_jobs` and `all_time_series_jobs` for easy management

### Schedules

**Automated Schedules** (`schedules/time_series.py`)
- `daily_time_series_schedule`: 10 AM UTC daily (RUNNING by default)
- `weekly_time_series_schedule`: 11 AM UTC Mondays (RUNNING by default)
- `monthly_time_series_schedule`: 12 PM UTC 1st of month (RUNNING by default)

**Schedule Implementation Details:**
- **Built-in Utilities**: Uses `build_schedule_from_partitioned_job` for maximum simplicity
- **Default Status**: `DefaultScheduleStatus.RUNNING` for automatic execution
- **Timing Strategy**: Staggered timing to avoid resource conflicts
- **Partition Awareness**: Automatically handles partition selection for previous period
- **Collections**: Organized in `time_series_schedules` for easy management

### Sensors

**Dynamic Partition Management** (`sensors/time_series.py`)
- `sync_metric_contexts_partition_sensor`: Comprehensive metric context discovery and management

**Sensor Implementation Details:**
- **Interval**: 15-minute minimum interval for balanced discovery vs performance
- **Default Status**: `DefaultSensorStatus.RUNNING` for automatic discovery
- **Discovery Logic**: Uses `discover_metric_contexts()` to query all active tenants/metrics
- **Partition Management**:
  - Adds new metric contexts via `AddDynamicPartitionsRequest`
  - Removes inactive contexts via `DeleteDynamicPartitionsRequest`
- **Comprehensive Logging**: Detailed logging of partition changes and statistics
- **Collections**: Organized in `time_series_sensors` for integration with main definitions

## Migration Benefits

### Scalability Improvements
1. **Dynamic Partitions**: Auto-discovery of new metrics/tenants
2. **Concurrent Execution**: Configurable parallelism per grain
3. **Efficient Backfills**: Date-aware partition execution
4. **Resource Pooling**: Shared database connections and clients

### Operational Excellence
1. **Rich Observability**: Detailed logging and metrics
2. **Failure Recovery**: Automatic retry with exponential backoff
3. **Idempotent Operations**: Safe re-execution of any partition
4. **Flexible Scheduling**: Grain-specific timing optimization

### Developer Experience
1. **Service-Oriented**: Reusable logic across assets
2. **Type Safety**: Pydantic models for all interfaces
3. **Clear Separation**: Assets, jobs, schedules, sensors
4. **Comprehensive Testing**: Partition-aware test strategies

## Backfill Strategy

### Date-Aware Execution
- **Daily**: Materializes every day in range (format: YYYY-MM-DD)
- **Weekly**: Materializes first day of each week in range (format: YYYY-W%W)
- **Monthly**: Materializes first day of each month in range (format: YYYY-MM)

### Multi-Dimensional Partition Backfills
Each asset uses multi-dimensional partitions combining time and metric context:
- Daily: `"2025-07-01|tenant_a::cpu_usage"` (pipe-separated format)
- Weekly: `"2025-W27|tenant_a::cpu_usage"` (Monday-based weeks, pipe-separated)
- Monthly: `"2025-07|tenant_a::cpu_usage"` (pipe-separated format)

**Partition Key Access**: Use `partition_key.keys_by_dimension` to access individual dimensions:
```python
date_str = context.partition_key.keys_by_dimension["date"]
metric_context_str = context.partition_key.keys_by_dimension["metric_context"]
```

### Backfill Commands
```bash
# Backfill specific date range for daily grain
dagster asset materialize --select metric_time_series_daily --partition-range 2025-07-01:2025-07-31

# Backfill weekly data for specific weeks
dagster asset materialize --select metric_time_series_weekly --partition-range 2025-W27:2025-W30

# Backfill monthly data for specific months
dagster asset materialize --select metric_time_series_monthly --partition-range 2025-07:2025-12

# Backfill all grains using combined job
dagster job execute --name all_grains_time_series_job
```

### Dynamic Partition Discovery
The `sync_metric_contexts_partition_sensor` enables automatic backfill scenarios:
- Discovery of new tenant/metric combinations
- Automatic partition creation for new contexts
- Removal of inactive metric contexts
- Consistent 15-minute discovery interval

## Configuration

### Resource Architecture
- `AppConfigResource`: Core application configuration from environment
- `DbResource`: Multi-tenant database access with connection pooling (profile: "micro")
- `sync_db`: Un-pooled database resource for sensors/schedules (NullPool, app_name: "asset_manager_daemon")
- `SnowflakeResource`: Pooled engine for snowflake assets/jobs
- `S3Resource` & `S3PickleIOManager`: Production S3 storage (conditional on bucket configuration)

### Resource Configuration Details
- **Connection Pooling Strategy**: Pooled engines for assets/jobs, non-pooled for daemon processes
- **Multi-Profile Support**: "micro" profile for lightweight database operations
- **Conditional S3**: S3 resources only added when `dagster_s3_bucket` is configured
- **App Name Differentiation**: Daemon processes use specific app_name for monitoring

### Environment Variables
- Standard Fulcrum Intelligence configuration
- `DAGSTER_S3_BUCKET`: Optional S3 bucket for production IO management
- Database connection settings via `DbResource` profiles
- Auth0 configuration for service-to-service authentication

## Testing Strategy

### Unit Tests
- Service logic testing with mocked dependencies
- Partition key parsing/formatting
- Sync type determination logic

### Integration Tests
- End-to-end asset execution
- Database operations
- Multi-partition scenarios

### Performance Tests
- Concurrent execution limits
- Memory usage patterns
- Database connection pooling

## Migration Checklist

### Core Implementation ✅ Complete
- [x] ✅ **Partitions**: Multi-dimensional time-based + dynamic metric contexts with MetricContext class
- [x] ✅ **Services**: Comprehensive SemanticSyncService with async operations and error handling
- [x] ✅ **Assets**: Daily/weekly/monthly time series with rich metadata and logging
- [x] ✅ **Jobs**: Grain-specific + combined jobs with asset selection and tagging
- [x] ✅ **Schedules**: Automated schedules using Dagster built-in utilities (RUNNING by default)
- [x] ✅ **Sensors**: Dynamic metric context discovery with 15-minute intervals
- [x] ✅ **Definitions**: Full integration with existing Dagster setup including resource management
- [x] ✅ **Resource Architecture**: Pooled/non-pooled database resources with conditional S3 support

### Remaining Implementation 🔄 In Progress
- [ ] 🔄 **Testing**: Comprehensive test suite for services, assets, and partition logic
- [ ] 🔄 **Documentation**: API documentation and operational runbooks
- [ ] 🔄 **Monitoring**: Observability dashboards and alerting configuration
- [ ] 🔄 **Deployment**: Production rollout strategy and migration plan

## Pattern Analysis Migration ✅ Complete

### Architecture

**Event-Driven Pattern Execution**
- Pattern analysis assets triggered by successful time series materializations
- Asset sensors monitor time series completion and automatically trigger pattern runs
- Single dynamic partition space: `metric_pattern_contexts` shared across all grains
- 2D partitions per grain: time dimension × `metric_pattern_context`

**Partition Strategy**
- `metric_pattern_contexts`: Single dynamic partition for all execution contexts (`"tenant_a::cpu_usage::performance_status"`)
- Multi-dimensional partitions combining time and pattern context:
  - Daily: `{"date": daily_partitions, "metric_pattern_context": metric_pattern_contexts}`
  - Weekly: `{"week": weekly_partitions, "metric_pattern_context": metric_pattern_contexts}`
  - Monthly: `{"month": monthly_partitions, "metric_pattern_context": metric_pattern_contexts}`

**MetricPatternContext Class**
- Pydantic model for parsing execution context strings
- Methods: `from_string()`, `to_string()`, `to_metric_context()`
- Format: `"tenant_a::cpu_usage::performance_status"`

### Services

**Pattern Orchestration Service** (`services/patterns.py`)
- Functional RORO pattern with async operations
- Core functions:
  - `run_patterns_for_metric()`: Main orchestration with standard/dimension pattern categorization
  - `run_single_pattern()`: Individual pattern execution with error handling
  - `fetch_pattern_data()`: Data fetching via `PatternDataOrganiser`
  - `store_pattern_result()`: Result storage via `PatternManager`
- Concurrent execution using `asyncio.gather()` for multiple patterns/dimensions

**PatternDataOrganiser** (`services/pattern_data_organiser.py`)
- Ported from `tasks_manager` with asset manager resource patterns
- Supports all data source types: metric time series, dimensional, multi-metric, targets
- Async data fetching with proper error handling and empty dataset detection

### Assets

**Pattern Analysis Assets** (`assets/patterns.py`)
- `pattern_run_daily`: Daily pattern analysis - fetches data directly from database
- `pattern_run_weekly`: Weekly pattern analysis - fetches data directly from database
- `pattern_run_monthly`: Monthly pattern analysis - fetches data directly from database

**Asset Implementation Details:**
- **Group Name**: "patterns" for logical organization
- **Data Independence**: Fetches data directly from database using partition date - no upstream asset dependencies
- **Partition Format**: `"2025-07-01|tenant_a::cpu_usage::performance_status"` (pipe-separated)
- **Metadata**: Comprehensive execution summary with tenant, metric, pattern, status, counts, duration
- **Service Integration**: Uses pattern orchestration service for all analysis logic

### Jobs

**Pattern Jobs** (`jobs/patterns.py`)
- `daily_patterns_job`: Executes daily pattern analysis assets
- `weekly_patterns_job`: Executes weekly pattern analysis assets
- `monthly_patterns_job`: Executes monthly pattern analysis assets
- `all_patterns_job`: Combined job for all pattern grains (backfills)

### Sensors

**Dynamic Partition Management** (`sensors/patterns.py`)
- `sync_metric_pattern_contexts_sensor`: Maintains `metric_pattern_contexts` partitions
  - Discovers active metric contexts and available patterns
  - Builds execution context keys: `tenant::metric::pattern`
  - 15-minute interval with conservative add-only approach

**Event-Driven Execution Sensors**
- `trigger_daily_patterns_on_time_series`: Triggers daily patterns on time series completion
- `trigger_weekly_patterns_on_time_series`: Triggers weekly patterns on time series completion
- `trigger_monthly_patterns_on_time_series`: Triggers monthly patterns on time series completion

**Sensor Implementation Details:**
- Monitor specific time series assets using `@asset_sensor`
- Ensure required dynamic partitions exist before triggering runs
- Generate `RunRequest`s for each available pattern for the materialized metric context
- Unique run keys prevent duplicate executions: `{grain}:{time}:{metric_context}:{pattern}`

### Backfill Strategy

**Pattern-Specific Backfills**
```bash
# Daily pattern backfill
dagster asset materialize --select pattern_run_daily --partition "2025-07-01|tenant_a::cpu_usage::performance_status"

# Weekly pattern range backfill
dagster asset materialize --select pattern_run_weekly --partition-range "2025-07-07|tenant_a::cpu_usage::forecasting:2025-07-28|tenant_a::cpu_usage::forecasting"

# Monthly pattern backfill
dagster asset materialize --select pattern_run_monthly --partition "2025-07|tenant_a::cpu_usage::drift_detection"

# All patterns for a grain
dagster job execute --name daily_patterns_job --partition "2025-07-01|tenant_a::cpu_usage::performance_status"
```

**Multi-Dimensional Partition Access**
```python
# In pattern assets
date_str = context.partition_key.keys_by_dimension["date"]
exec_ctx = MetricPatternContext.from_string(context.partition_key.keys_by_dimension["metric_pattern_context"])
```

### Migration Benefits

**Event-Driven Execution**
1. **Immediate Triggering**: Pattern analysis starts as soon as time series data is available
2. **Logical Dependency Management**: Asset sensors ensure time series completion before pattern execution
3. **No Clock Skew**: Eliminates schedule-based delays and timing issues
4. **Selective Execution**: Only runs patterns for metrics that have new data
5. **Data Independence**: Pattern assets fetch data directly from database, no asset data dependencies

**Operational Excellence**
1. **Comprehensive Logging**: Detailed execution tracking with performance metrics
2. **Error Isolation**: Individual pattern failures don't stop other patterns
3. **Metadata Rich**: Complete execution context in asset materialization metadata
4. **Simplified Architecture**: No complex partition mapping or upstream asset dependencies

## Next Steps

1. **Stories Migration**: Add story generation assets with pattern dependencies
2. **Cross-Grain Dependencies**: Weekly/monthly depending on daily completion
3. **Advanced Backfilling**: Smart dependency resolution for historical loads
4. **Performance Optimization**: Caching and connection pooling tuning

## Sync Logic Deep Dive

### Intelligent Sync Type Determination
- **FULL vs INCREMENTAL**: Per-metric and per-dimension determination using `SemanticManager.metric_sync_status`
- **Lookback Periods**: Dynamic calculation based on grain and sync type
  - Daily: 365 days (FULL) / 90 days (INCREMENTAL)
  - Weekly: 104 weeks (FULL) / 24 weeks (INCREMENTAL)
  - Monthly: 5 years (FULL) / 1 year (INCREMENTAL)

### Multi-Dimensional Processing
- **Base Metric Sync**: Primary metric data fetching and storage
- **Dimensional Sync**: Parallel processing of all metric dimensions
- **Error Isolation**: Failed dimensions don't stop base metric processing
- **Transaction Management**: Proper database transactions with rollback on failures

### Performance Optimizations
- **Async Operations**: Full async/await throughout the pipeline
- **Connection Pooling**: Separate pooling strategies for different workload types
- **Batch Operations**: Bulk upsert operations for time series data
- **Resource Management**: Proper cleanup of database sessions and tenant contexts

## File Structure

```
asset_manager/
├── assets/
│   ├── __init__.py                 # Asset exports including snowflake cache
│   └── time_series.py              # Multi-dimensional time series assets
├── jobs/
│   ├── __init__.py                 # Job exports for all asset types
│   └── time_series.py              # Grain-specific and combined execution jobs
├── partitions.py                   # Multi-dimensional partition definitions with MetricContext
├── schedules/
│   ├── __init__.py                 # Schedule exports including snowflake schedules
│   └── time_series.py              # Built-in utility-based automated schedules
├── sensors/
│   ├── __init__.py                 # Sensor exports including snowflake sensors
│   └── time_series.py              # Dynamic partition management sensor
├── services/
│   ├── semantic_sync.py            # Complete sync orchestration service with models
│   └── utils.py                    # Tenant discovery and metric fetching utilities
└── definitions.py                  # Main Dagster definitions with resource architecture
```

## Summary

This migration successfully implements a production-ready semantic sync pipeline in Dagster with:

- **Scalable Architecture**: Multi-dimensional partitions with automatic discovery
- **Operational Excellence**: Comprehensive logging, error handling, and recovery
- **Performance Optimization**: Async operations with intelligent sync strategies
- **Production Readiness**: Resource management, conditional S3, and proper separation of concerns

The implementation provides a solid foundation for the full E2E analytics pipeline while maintaining compatibility with existing snowflake cache patterns.
