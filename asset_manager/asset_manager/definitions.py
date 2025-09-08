"""Main Dagster definitions for asset manager."""

from typing import Any, cast

from dagster import Definitions
from dagster_aws.s3 import S3PickleIOManager, S3Resource

from asset_manager.assets import (
    metric_semantic_values,
    metric_stories_daily,
    metric_stories_monthly,
    metric_stories_weekly,
    metric_time_series_daily,
    metric_time_series_monthly,
    metric_time_series_weekly,
    pattern_run_daily,
    pattern_run_monthly,
    pattern_run_weekly,
    snowflake_metric_cache,
)
from asset_manager.jobs import (
    grain_jobs,
    pattern_grain_jobs,
    snowflake_cache_job,
    story_grain_jobs,
)
from asset_manager.resources import AppConfigResource, DbResource, SnowflakeResource
from asset_manager.schedules import (
    daily_snowflake_cache_schedule,
    monthly_snowflake_cache_schedule,
    time_series_schedules,
    weekly_snowflake_cache_schedule,
)
from asset_manager.sensors import (
    sync_dynamic_partitions,
    sync_metric_contexts_partition_sensor,
    sync_metric_pattern_contexts_sensor,
    trigger_daily_patterns_on_time_series,
    trigger_daily_stories_on_pattern_runs,
    trigger_monthly_patterns_on_time_series,
    trigger_monthly_stories_on_pattern_runs,
    trigger_weekly_patterns_on_time_series,
    trigger_weekly_stories_on_pattern_runs,
)

# Define all assets
all_assets = [
    # semantic cache assets
    metric_semantic_values,
    snowflake_metric_cache,
    # time series assets
    metric_time_series_daily,
    metric_time_series_weekly,
    metric_time_series_monthly,
    # pattern analysis assets
    pattern_run_daily,
    pattern_run_weekly,
    pattern_run_monthly,
    # story generation assets
    metric_stories_daily,
    metric_stories_weekly,
    metric_stories_monthly,
]

# Define resources
app_config = AppConfigResource.from_env()
resources = {
    "app_config": app_config,
    "snowflake": SnowflakeResource(app_config=app_config),  # pooled engine for assets/jobs
    "db": DbResource(app_config=app_config, profile="micro"),
    # un-pooled engine for sensors/schedules
    "sync_db": DbResource(
        app_config=app_config,
        profile="micro",
        engine_overrides_json='{"poolclass": "NullPool"}',
        app_name="asset_manager_daemon",
    ),
}

# Add S3 IO Manager only if S3 bucket is configured (prod environment)
if app_config.settings.dagster_s3_bucket:
    # Add the missing S3 resource
    s3_resource = S3Resource()
    resources["s3"] = s3_resource
    resources["io_manager"] = cast(
        Any,
        S3PickleIOManager(
            s3_resource=s3_resource,
            s3_bucket=app_config.settings.dagster_s3_bucket,
            s3_prefix="dagster/io",
        ),
    )

# Define jobs
jobs = [snowflake_cache_job] + grain_jobs + pattern_grain_jobs + story_grain_jobs

# Define schedules
schedules = [
    daily_snowflake_cache_schedule,
    weekly_snowflake_cache_schedule,
    monthly_snowflake_cache_schedule,
] + time_series_schedules

# Define sensors
sensors = [
    sync_dynamic_partitions,
    sync_metric_contexts_partition_sensor,
    sync_metric_pattern_contexts_sensor,
    trigger_daily_patterns_on_time_series,
    trigger_weekly_patterns_on_time_series,
    trigger_monthly_patterns_on_time_series,
    trigger_daily_stories_on_pattern_runs,
    trigger_weekly_stories_on_pattern_runs,
    trigger_monthly_stories_on_pattern_runs,
]

# Main definitions object that Dagster will discover
defs = Definitions(assets=all_assets, resources=resources, jobs=jobs, schedules=schedules, sensors=sensors)
