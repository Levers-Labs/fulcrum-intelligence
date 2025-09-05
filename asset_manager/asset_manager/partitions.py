"""Partition definitions for asset manager.

Supports both cache-specific partitions and time series partitions:
- cache_tenant_grain_metric: "<tenant_id>::<grain>::<metric_id>" (legacy)
- metric_contexts: "<tenant_id>::<metric_id>" (time series)
- Time-based partitions: daily, weekly, monthly
- Multi-dimensional partitions for time series assets

Uses MetricContext class for consistent metric context handling.
"""

from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    WeeklyPartitionsDefinition,
)
from pydantic import BaseModel

KEY_SEP = "::"


class MetricContext(BaseModel):
    """Parse and handle metric context strings."""

    tenant: str
    metric: str

    @property
    def key(self) -> str:
        """Get the key for the metric context."""
        return KEY_SEP.join([self.tenant, self.metric])

    @classmethod
    def from_string(cls, context_str: str) -> "MetricContext":
        """Parse 'tenant_a::cpu_usage' format."""
        parts = context_str.split("::")
        if len(parts) != 2:
            raise ValueError(f"Invalid metric context: {context_str}")
        return cls(tenant=parts[0], metric=parts[1])

    def to_string(self) -> str:
        """Convert back to string format."""
        return self.key

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return self.to_string()


# ============================================
# TIME PARTITIONS (Per Grain)
# ============================================

daily_partitions = DailyPartitionsDefinition(start_date="2025-07-01", fmt="%Y-%m-%d")

weekly_partitions = WeeklyPartitionsDefinition(start_date="2025-07-01", day_offset=1)

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-07", fmt="%Y-%m")

# ============================================
# DYNAMIC PARTITIONS
# ============================================

# Shared metric contexts across all grains (format: "tenant_a::cpu_usage")
metric_contexts_partition = DynamicPartitionsDefinition(name="metric_contexts")

# Legacy cache partition (keep for existing snowflake assets)
cache_tenant_grain_metric_partition = DynamicPartitionsDefinition(name="cache_tenant_grain_metric")

# ============================================
# MULTI-DIMENSIONAL PARTITIONS
# ============================================

# For time series assets (2D: date × tenant::metric)
daily_time_series_partitions = MultiPartitionsDefinition(
    {"date": daily_partitions, "metric_context": metric_contexts_partition}
)

weekly_time_series_partitions = MultiPartitionsDefinition(
    {"week": weekly_partitions, "metric_context": metric_contexts_partition}
)

monthly_time_series_partitions = MultiPartitionsDefinition(
    {"month": monthly_partitions, "metric_context": metric_contexts_partition}
)

# ============================================
# HELPER FUNCTIONS
# ============================================


def to_tenant_grain_metric_key(tenant_identifier: str, grain: str, metric_id: str) -> str:
    """Combine tenant, grain, and metric into a single partition key."""
    return KEY_SEP.join([tenant_identifier, grain, metric_id])


def parse_tenant_grain_metric_key(key: str) -> tuple[str, str, str]:
    """Parse combined partition key into tenant, grain, and metric components."""
    tenant_identifier, grain, metric_id = key.split(KEY_SEP, 2)
    return tenant_identifier, grain, metric_id
