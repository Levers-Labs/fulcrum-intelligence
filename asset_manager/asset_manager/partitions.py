"""Partition definitions using two dynamic dimensions.

We compress three logical dimensions (tenant, metric, grain) into two physical
ones supported by Dagster's MultiPartitionsDefinition:

- tenant_metric: "<tenant_id>::<metric_id>"
- tenant_grain:  "<tenant_id>::<grain>"

Helper functions are provided to compose and parse keys consistently.
"""

from dagster import DynamicPartitionsDefinition, MultiPartitionsDefinition

KEY_SEP = "::"


def to_tenant_metric_key(tenant_identifier: str, metric_id: str) -> str:
    return f"{tenant_identifier}{KEY_SEP}{metric_id}"


def to_tenant_grain_key(tenant_identifier: str, grain: str) -> str:
    return f"{tenant_identifier}{KEY_SEP}{grain}"


def parse_tenant_metric_key(key: str) -> tuple[str, str]:
    tenant_identifier, metric_id = key.split(KEY_SEP, 1)
    return tenant_identifier, metric_id


def parse_tenant_grain_key(key: str) -> tuple[str, str]:
    tenant_identifier, grain = key.split(KEY_SEP, 1)
    return tenant_identifier, grain


tenant_metric_partition = DynamicPartitionsDefinition(name="tenant_metric")
tenant_grain_partition = DynamicPartitionsDefinition(name="tenant_grain")

multi_partitions_def = MultiPartitionsDefinition(
    {
        "tenant_metric": tenant_metric_partition,
        "tenant_grain": tenant_grain_partition,
    }
)
