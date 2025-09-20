"""Schedules for time series assets.

Provides automated scheduling for daily, weekly, and monthly semantic sync
assets using Dagster's built-in schedule utilities for maximum simplicity.
"""

from dagster import DefaultScheduleStatus, build_schedule_from_partitioned_job

from asset_manager.jobs.time_series import daily_time_series_job, monthly_time_series_job, weekly_time_series_job

# ============================================
# SIMPLIFIED SCHEDULES USING DAGSTER UTILITIES
# ============================================

# Daily schedule - runs at 9 AM (UTC), 2 AM (PDT/PST), 2:30 pm (IST) daily for previous day's data
daily_time_series_schedule = build_schedule_from_partitioned_job(
    job=daily_time_series_job,
    hour_of_day=9,
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.RUNNING,
)

# Weekly schedule - runs at 10 AM (UTC), 3 AM (PDT/PST), 3:30 pm (IST) on Mondays for previous week's data
weekly_time_series_schedule = build_schedule_from_partitioned_job(
    job=weekly_time_series_job,
    hour_of_day=10,
    minute_of_hour=0,
    day_of_week=1,  # Monday
    default_status=DefaultScheduleStatus.RUNNING,
)

# Monthly schedule - runs at 11 AM (UTC), 4 AM (PDT/PST), 4:30 pm (IST) on 1st of each month for previous month's data
monthly_time_series_schedule = build_schedule_from_partitioned_job(
    job=monthly_time_series_job,
    hour_of_day=11,
    minute_of_hour=0,
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
