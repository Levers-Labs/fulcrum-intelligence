"""Schedules for time series assets.

Provides automated scheduling for daily, weekly, and monthly semantic sync
assets using Dagster's built-in schedule utilities for maximum simplicity.
"""

from dagster import DefaultScheduleStatus, build_schedule_from_partitioned_job

from asset_manager.jobs.time_series import daily_time_series_job, monthly_time_series_job, weekly_time_series_job

# ============================================
# SIMPLIFIED SCHEDULES USING DAGSTER UTILITIES
# ============================================

# Daily schedule - runs at 11:30 AM (UTC), 3:30 AM (PST), 5:00 PM (IST)
daily_time_series_schedule = build_schedule_from_partitioned_job(
    job=daily_time_series_job,
    hour_of_day=11,
    minute_of_hour=30,
    default_status=DefaultScheduleStatus.RUNNING,
)

# Weekly schedule - runs at 12:30 PM (UTC), 4:30 AM (PST), 6:00 PM (IST) on Mondays
weekly_time_series_schedule = build_schedule_from_partitioned_job(
    job=weekly_time_series_job,
    hour_of_day=12,
    minute_of_hour=30,
    day_of_week=1,  # Monday
    default_status=DefaultScheduleStatus.RUNNING,
)

# Monthly schedule - runs at 1:30 PM (UTC), 5:30 AM (PST), 7:00 PM (IST) on 1st of each month
monthly_time_series_schedule = build_schedule_from_partitioned_job(
    job=monthly_time_series_job,
    hour_of_day=13,
    minute_of_hour=30,
    day_of_month=1,
    default_status=DefaultScheduleStatus.RUNNING,
)

# ============================================
# SCHEDULE COLLECTIONS
# ============================================

# Active schedules that run automatically
time_series_schedules = [
    daily_time_series_schedule,
    weekly_time_series_schedule,
    monthly_time_series_schedule,
]
