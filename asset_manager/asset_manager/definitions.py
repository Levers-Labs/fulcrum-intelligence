"""Main Dagster definitions for asset manager."""

from dagster import Definitions

from asset_manager.assets import metric_semantic_values, snowflake_metric_cache
from asset_manager.jobs import snowflake_cache_job
from asset_manager.resources import AppConfigResource, DbResource, SnowflakeResource
from asset_manager.schedules import daily_snowflake_cache_schedule
from asset_manager.sensors import sync_dynamic_partitions

# Define all assets
all_assets = [
    # semantic cache assets
    metric_semantic_values,
    snowflake_metric_cache,
]

# Define resources
app_config = AppConfigResource.from_env()
resources = {
    "app_config": app_config,
    "snowflake": SnowflakeResource(app_config=app_config),  # pooled engine for assets/jobs
    "db": DbResource(app_config=app_config, profile="prod"),
    # unpooled engine for sensors/schedules
    "sync_db": DbResource(app_config=app_config, profile="prod", engine_overrides_json='{"poolclass": "NullPool"}'),
}

# Define jobs
jobs = [snowflake_cache_job]

# Define schedules
schedules = [daily_snowflake_cache_schedule]

# Define sensors
sensors = [sync_dynamic_partitions]

# Main definitions object that Dagster will discover
defs = Definitions(assets=all_assets, resources=resources, jobs=jobs, schedules=schedules, sensors=sensors)
