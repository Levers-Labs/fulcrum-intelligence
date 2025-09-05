"""Jobs for time series assets.

Defines jobs that group time series assets by grain for efficient
execution and backfilling. Each job is partitioned and can be
scheduled or triggered manually.
"""

from dagster import AssetSelection, define_asset_job

from asset_manager.partitions import (
    daily_time_series_partitions,
    monthly_time_series_partitions,
    weekly_time_series_partitions,
)

# ============================================
# DAILY TIME SERIES JOB
# ============================================

daily_time_series_job = define_asset_job(
    name="daily_time_series_job",
    selection=AssetSelection.assets("metric_time_series_daily"),
    partitions_def=daily_time_series_partitions,
    description="Process daily metric time series data",
    tags={"grain": "daily", "type": "time_series", "owner": "data-platform"},
)

# ============================================
# WEEKLY TIME SERIES JOB
# ============================================

weekly_time_series_job = define_asset_job(
    name="weekly_time_series_job",
    selection=AssetSelection.assets("metric_time_series_weekly"),
    partitions_def=weekly_time_series_partitions,
    description="Process weekly metric time series data",
    tags={"grain": "weekly", "type": "time_series", "owner": "data-platform"},
)

# ============================================
# MONTHLY TIME SERIES JOB
# ============================================

monthly_time_series_job = define_asset_job(
    name="monthly_time_series_job",
    selection=AssetSelection.assets("metric_time_series_monthly"),
    partitions_def=monthly_time_series_partitions,
    description="Process monthly metric time series data",
    tags={"grain": "monthly", "type": "time_series", "owner": "data-platform"},
)

# ============================================
# JOB COLLECTIONS
# ============================================

# INDIVIDUAL GRAIN JOBS
grain_jobs = [
    daily_time_series_job,
    weekly_time_series_job,
    monthly_time_series_job,
]
