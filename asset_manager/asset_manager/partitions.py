"""Partition definitions for Snowflake cache assets.

Uses a single dynamic partition dimension that combines tenant, grain, and metric:
- cache_tenant_grain_metric: "<tenant_id>::<grain>::<metric_id>"

Helper functions are provided to compose and parse keys consistently.
"""

from dagster import DynamicPartitionsDefinition

KEY_SEP = "::"


def to_tenant_grain_metric_key(tenant_identifier: str, grain: str, metric_id: str) -> str:
    """Combine tenant, grain, and metric into a single partition key."""
    return KEY_SEP.join([tenant_identifier, grain, metric_id])


def parse_tenant_grain_metric_key(key: str) -> tuple[str, str, str]:
    """Parse combined partition key into tenant, grain, and metric components."""
    tenant_identifier, grain, metric_id = key.split(KEY_SEP, 2)
    return tenant_identifier, grain, metric_id


cache_tenant_grain_metric_partition = DynamicPartitionsDefinition(name="cache_tenant_grain_metric")
