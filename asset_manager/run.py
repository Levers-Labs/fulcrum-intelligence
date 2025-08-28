"""Programmatic runners for Dagster jobs/assets (debugger-friendly).

Examples:
  python run.py job --job full_pipeline --tenant 123 --metric revenue --grain day
  python run.py asset --asset metric_semantic_values --tenant 123 --metric revenue --grain day
"""

from __future__ import annotations

import argparse
import os
import sys

from dagster import DagsterInstance, materialize

# Assets and definitions
from asset_manager.assets import metric_semantic_values, snowflake_metric_cache
from asset_manager.definitions import defs
from asset_manager.partitions import cache_tenant_grain_metric_partition, to_tenant_grain_metric_key
from asset_manager.resources import AppConfigResource, DbResource, SnowflakeResource

sys.path.append(os.path.dirname(__file__))


def build_partition_key(tenant: str, grain: str, metric: str) -> str:
    return to_tenant_grain_metric_key(tenant, grain, metric)


def run_job(job_name: str, tenant: str, grain: str, metric: str) -> int:
    job = defs.get_job_def(job_name)
    partition_key = build_partition_key(tenant, grain, metric)
    # Ensure dynamic partition key exists in instance
    instance = DagsterInstance.get()
    instance.add_dynamic_partitions(cache_tenant_grain_metric_partition.name, [partition_key])
    result = job.execute_in_process(partition_key=partition_key, instance=instance)
    return 0 if result.success else 1


def run_asset(asset_name: str, tenant: str, grain: str, metric: str) -> int:
    assets_map = {
        "metric_semantic_values": metric_semantic_values,
        "snowflake_metric_cache": snowflake_metric_cache,
    }
    asset = assets_map.get(asset_name)
    if asset is None:
        raise SystemExit(f"Unknown asset: {asset_name}")

    partition_key = build_partition_key(tenant, grain, metric)

    # Use direct materialize with resources to allow IDE debugging
    app_config = AppConfigResource.from_env()
    snowflake = SnowflakeResource(app_config=app_config)
    db_ = DbResource(app_config=app_config)

    # Ensure dynamic partition key exists in instance
    instance = DagsterInstance.get()
    instance.add_dynamic_partitions(cache_tenant_grain_metric_partition.name, [partition_key])

    result = materialize(
        [asset],
        resources={
            "app_config": app_config,
            "snowflake": snowflake,
            "db": db_,
        },
        partition_key=partition_key,
        instance=instance,
    )
    return 0 if result.success else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Dagster jobs/assets programmatically")
    sub = parser.add_subparsers(dest="mode", required=True)

    job_p = sub.add_parser("job", help="Execute a Dagster job")
    job_p.add_argument("--job", default="full_pipeline")
    job_p.add_argument("--tenant", required=True)
    job_p.add_argument("--metric", required=True)
    job_p.add_argument("--grain", required=True)

    asset_p = sub.add_parser("asset", help="Materialize a single asset")
    asset_p.add_argument("--asset", required=True, choices=["metric_semantic_values", "snowflake_metric_cache"])
    asset_p.add_argument("--tenant", required=True)
    asset_p.add_argument("--metric", required=True)
    asset_p.add_argument("--grain", required=True)

    args = parser.parse_args()

    if args.mode == "job":
        return run_job(args.job, args.tenant, args.grain, args.metric)
    else:
        return run_asset(args.asset, args.tenant, args.grain, args.metric)


if __name__ == "__main__":
    raise SystemExit(main())
