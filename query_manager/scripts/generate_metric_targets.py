"""Script to generate metric target values based on existing time series data."""

import logging
import random
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import select

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.db.config import open_async_session
from query_manager.semantic_manager.crud import SemanticManager
from query_manager.semantic_manager.models import MetricTimeSeries

logger = logging.getLogger(__name__)


def generate_streak_length() -> int:
    """Generate a random streak length between 3 and 10 days."""
    return random.randint(3, 10)  # noqa


def generate_target_value(actual_value: float, variance_percent: float = 5.0) -> float:
    """Generate a target value within ±variance_percent of the actual value."""
    variance = actual_value * (variance_percent / 100)
    return actual_value + random.uniform(-variance, variance)  # noqa


def generate_streak_targets(
    start_date: date,
    streak_length: int,
    time_series_data: pd.DataFrame,
    is_stable: bool,
    variance_percent: float = 5.0,
) -> list[tuple[date, float]]:
    """Generate a streak of target values based on actual time series data."""
    values: list[tuple[date, float]] = []
    current_date = start_date

    # Get the actual values for this streak period
    streak_data = time_series_data[
        (time_series_data["date"] >= start_date)
        & (time_series_data["date"] < start_date + timedelta(days=streak_length))
    ]

    if streak_data.empty:
        return values

    if is_stable:
        # For stable streaks, use a single target value based on the average of actual values
        avg_value = streak_data["value"].mean()
        target_value = generate_target_value(avg_value, variance_percent=2.0)
        for _ in range(streak_length):
            values.append((current_date, target_value))
            current_date += timedelta(days=1)
    else:
        # For varying streaks, generate targets based on each day's actual value
        for _, row in streak_data.iterrows():
            value = generate_target_value(row["value"], variance_percent=variance_percent)
            values.append((row["date"], value))
            current_date = row["date"] + timedelta(days=1)

    return values


async def get_time_series_data(
    semantic_manager: SemanticManager,
    metric_id: str,
    tenant_id: int,
    start_date: date,
    end_date: date,
    grain: Granularity,
) -> pd.DataFrame:
    """Fetch existing time series data for the metric."""
    time_series = await semantic_manager.get_metric_time_series(
        metric_id=metric_id,
        grain=grain,
        start_date=start_date,
        end_date=end_date,
    )

    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame([{"date": ts.date, "value": ts.value} for ts in time_series])

    if df.empty:
        logger.warning("No time series data found for metric %s with grain %s", metric_id, grain)
        return df
    return df.sort_values("date")


async def get_all_metrics(session) -> list[str]:
    """Get all unique metric IDs from time series data."""
    # Query to get distinct metric_ids from metric_time_series
    query = select(MetricTimeSeries.metric_id).distinct()  # type: ignore
    result = await session.execute(query)
    return [row[0] for row in result]


async def generate_metric_targets(
    session,
    metric_id: str,
    tenant_id: int,
    start_date: date,
    end_date: date,
    grain: Granularity = Granularity.DAY,
    variance_percent: float = 5.0,
) -> list[dict[str, Any]]:
    """
    Generate metric target values based on existing time series data.

    Args:
        session: Database session
        metric_id: ID of the metric
        tenant_id: ID of the tenant
        start_date: Start date for the targets
        end_date: End date for the targets
        grain: Time granularity (default: daily)
        variance_percent: Maximum variance percentage from actual values (default: 5%)

    Returns:
        List of target dictionaries ready for bulk upsert
    """
    semantic_manager = SemanticManager(session)

    # Get existing time series data
    time_series_data = await get_time_series_data(
        semantic_manager,
        metric_id,
        tenant_id,
        start_date,
        end_date,
        grain,
    )

    if time_series_data.empty:
        logger.warning("No time series data found for metric %s with grain %s", metric_id, grain)
        return []

    target_data = []
    current_date = start_date

    while current_date <= end_date:
        # Generate a streak
        streak_length = generate_streak_length()
        # Randomly decide if this streak should be stable or varying
        is_stable = random.random() < 0.7  # 70% chance of being stable # noqa

        # Generate target values for the streak
        streak_targets = generate_streak_targets(
            current_date,
            streak_length,
            time_series_data,
            is_stable,
            variance_percent,
        )

        # Add the streak targets to our target data
        for date_value, value in streak_targets:
            if date_value <= end_date:  # Only add values within our date range
                target_data.append(
                    {
                        "metric_id": metric_id,
                        "grain": grain,
                        "target_date": date_value,
                        "target_value": value,
                    }
                )

        # Move to the next date after the streak
        current_date += timedelta(days=streak_length)

    return target_data


async def main():
    """Main function to generate and store metric target values."""
    tenant_id = 1
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    set_tenant_id(tenant_id)

    async with open_async_session("query_metric_targets") as session:
        try:
            semantic_manager = SemanticManager(session)

            # Get all metrics
            metrics = await get_all_metrics(session)
            logger.info("Found %d metrics to process", len(metrics))

            total_processed = 0
            total_failed = 0

            # Process each metric and grain combination
            for metric_id in metrics:
                logger.info("Processing metric: %s", metric_id)

                for grain in grains:
                    logger.info("Generating targets for grain: %s", grain)

                    # Generate target data
                    target_data = await generate_metric_targets(
                        session,
                        metric_id=metric_id,
                        tenant_id=tenant_id,
                        start_date=start_date,
                        end_date=end_date,
                        grain=grain,
                    )

                    if not target_data:
                        logger.warning("No target data generated for %s with grain %s", metric_id, grain)
                        continue

                    # Store the data using bulk upsert
                    stats = await semantic_manager.metric_target.bulk_upsert_targets(
                        targets=target_data, batch_size=1000  # Process in batches of 1000
                    )

                    total_processed += stats["processed"]
                    total_failed += stats["failed"]

                    logger.info("Processed %d targets for %s with grain %s", stats["processed"], metric_id, grain)
                    if stats["failed"] > 0:
                        logger.warning(
                            "Warning: %d targets failed for %s with grain %s", stats["failed"], metric_id, grain
                        )

            logger.info("Summary:")
            logger.info("Total targets processed: %d", total_processed)
            logger.info("Total targets failed: %d", total_failed)
        finally:
            reset_context()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
