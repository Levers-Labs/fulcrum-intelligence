"""Jobs for story generation assets.

Defines jobs that group story generation assets by grain for efficient
execution and backfilling. Each job is partitioned and can be triggered
by asset sensors or run manually.
"""

from dagster import AssetSelection, define_asset_job

from asset_manager.partitions import daily_pattern_partitions, monthly_pattern_partitions, weekly_pattern_partitions
from commons.models.enums import Granularity

# ============================================
# DAILY STORY GENERATION JOB
# ============================================

daily_stories_job = define_asset_job(
    name="daily_stories_job",
    selection=AssetSelection.assets("metric_stories_daily"),
    partitions_def=daily_pattern_partitions,
    description="Generate daily stories for selected execution contexts",
    tags={"grain": Granularity.DAY.value, "type": "story_generation", "owner": "data-platform"},
)

# ============================================
# WEEKLY STORY GENERATION JOB
# ============================================

weekly_stories_job = define_asset_job(
    name="weekly_stories_job",
    selection=AssetSelection.assets("metric_stories_weekly"),
    partitions_def=weekly_pattern_partitions,
    description="Generate weekly stories for selected execution contexts",
    tags={"grain": Granularity.WEEK.value, "type": "story_generation", "owner": "data-platform"},
)

# ============================================
# MONTHLY STORY GENERATION JOB
# ============================================

monthly_stories_job = define_asset_job(
    name="monthly_stories_job",
    selection=AssetSelection.assets("metric_stories_monthly"),
    partitions_def=monthly_pattern_partitions,
    description="Generate monthly stories for selected execution contexts",
    tags={"grain": Granularity.MONTH.value, "type": "story_generation", "owner": "data-platform"},
)

# ============================================
# JOB COLLECTIONS
# ============================================

# Story grain jobs for individual execution
story_grain_jobs = [
    daily_stories_job,
    weekly_stories_job,
    monthly_stories_job,
]
