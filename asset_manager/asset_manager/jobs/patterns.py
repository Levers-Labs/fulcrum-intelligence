"""Jobs for pattern analysis assets.

Defines jobs that group pattern analysis assets by grain for efficient
execution and backfilling. Each job is partitioned and can be triggered
by asset sensors or run manually.
"""

from dagster import AssetSelection, define_asset_job

from asset_manager.partitions import daily_pattern_partitions, monthly_pattern_partitions, weekly_pattern_partitions

# ============================================
# DAILY PATTERN ANALYSIS JOB
# ============================================

daily_patterns_job = define_asset_job(
    name="daily_patterns_job",
    selection=AssetSelection.assets("pattern_run_daily"),
    partitions_def=daily_pattern_partitions,
    description="Execute daily pattern analysis for selected execution contexts",
    tags={"grain": "daily", "type": "pattern_analysis", "owner": "data-platform"},
)

# ============================================
# WEEKLY PATTERN ANALYSIS JOB
# ============================================

weekly_patterns_job = define_asset_job(
    name="weekly_patterns_job",
    selection=AssetSelection.assets("pattern_run_weekly"),
    partitions_def=weekly_pattern_partitions,
    description="Execute weekly pattern analysis for selected execution contexts",
    tags={"grain": "weekly", "type": "pattern_analysis", "owner": "data-platform"},
)

# ============================================
# MONTHLY PATTERN ANALYSIS JOB
# ============================================

monthly_patterns_job = define_asset_job(
    name="monthly_patterns_job",
    selection=AssetSelection.assets("pattern_run_monthly"),
    partitions_def=monthly_pattern_partitions,
    description="Execute monthly pattern analysis for selected execution contexts",
    tags={"grain": "monthly", "type": "pattern_analysis", "owner": "data-platform"},
)

# ============================================
# JOB COLLECTIONS
# ============================================

# Pattern grain jobs for individual execution
pattern_grain_jobs = [
    daily_patterns_job,
    weekly_patterns_job,
    monthly_patterns_job,
]
