"""Partition definitions for Snowflake cache assets.

Uses a single dynamic partition dimension that combines tenant, metric, and grain:
- cache_tenant_metric_grain: "<tenant_id>::<metric_id>::<grain>"

Helper functions are provided to compose and parse keys consistently.
"""

from dagster import DynamicPartitionsDefinition

KEY_SEP = "::"


def to_tenant_metric_grain_key(tenant_identifier: str, metric_id: str, grain: str) -> str:
    """Combine tenant, metric, and grain into a single partition key."""
    return KEY_SEP.join([tenant_identifier, metric_id, grain])


def parse_tenant_metric_grain_key(key: str) -> tuple[str, str, str]:
    """Parse combined partition key into tenant, metric, and grain components."""
    tenant_identifier, metric_id, grain = key.split(KEY_SEP, 2)
    return tenant_identifier, metric_id, grain


cache_tenant_metric_grain_partition = DynamicPartitionsDefinition(name="cache_tenant_metric_grain")
