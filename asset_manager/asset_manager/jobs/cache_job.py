"""Job to materialize semantic extraction and Snowflake loading for all partitions."""

from dagster import AssetSelection, define_asset_job

from asset_manager.partitions import cache_tenant_metric_grain_partition

snowflake_cache_job = define_asset_job(
    name="snowflake_cache",
    description="Materialize metric values and load into Snowflake for all partitions",
    selection=AssetSelection.assets("metric_semantic_values", "snowflake_metric_cache"),
    partitions_def=cache_tenant_metric_grain_partition,
)
