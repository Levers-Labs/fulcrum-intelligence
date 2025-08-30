"""
Load V2 Mock Stories Script

This script generates and persists v2 mock stories using patterns for testing and development purposes.
Unlike v1 stories which use story groups, v2 stories are generated from pattern analysis results.

The script:
1. Generates mock pattern results for each specified pattern
2. Passes these to story evaluators to create stories
3. Persists the generated stories to the database

Usage:
    python -m story_manager.scripts.load_v2_mock_stories <tenant_id> <metric_id> [options]

Example:
    python -m story_manager.scripts.load_v2_mock_stories 1 revenue_metric --patterns performance_status,
    historical_performance --grains DAY,WEEK
"""

import argparse
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.grain_utils import GrainPeriodCalculator
from story_manager.core.dependencies import get_query_manager_client
from story_manager.db.config import open_async_session
from story_manager.mocks.v2.main import MockStoryServiceV2

logger = logging.getLogger(__name__)


async def load_mock_stories_v2(
    db_session: AsyncSession,
    tenant_id: int,
    metric: dict[str, Any],
    pattern: str | None = None,
    grain: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """
    Load v2 mock stories using patterns.

    Args:
        db_session: Database session
        tenant_id: The tenant ID
        metric: Metric dictionary with details
        pattern: Pattern name to generate (if None, generates all patterns)
        grain: Granularity to generate stories for
        start_date: Start date for story generation (optional)
        end_date: End date for story generation (optional)
    """
    # Default patterns if none specified
    if pattern:
        patterns = [pattern.strip()]
    else:
        patterns = ["performance_status", "historical_performance", "dimension_analysis"]

    # Default grains if none specified
    if grain:
        grains = [Granularity[grain.strip().upper()]]
    else:
        grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    logger.info(f"Loading v2 mock stories for metric {metric['metric_id']}")
    logger.info(f"Pattern: {patterns}")
    logger.info(f"Grain: {grains}")
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")

    # Handle date logic (matching v1 script logic)
    today = date.today()

    # Validate date parameters
    if end_date is not None and start_date is None:
        raise ValueError("start_date must be provided when end_date is provided")

    if start_date is not None and end_date is None:
        end_date = today
        logger.info(f"No end_date provided, defaulting to today: {end_date}")

    # Calculate dates for each grain
    grain_dates: dict[Granularity, list[date]] = {}

    for grain in grains:
        if start_date is not None:
            # When using date range, get all dates within the range and align them properly
            raw_dates = GrainPeriodCalculator.get_dates_for_range(
                grain, start_date=start_date, end_date=end_date  # type: ignore
            )

            # Align dates properly for each grain
            aligned_dates = []
            for raw_date in raw_dates:
                if grain == Granularity.WEEK:
                    # Ensure week dates are always Mondays
                    days_since_monday = raw_date.weekday()  # Monday=0, Sunday=6
                    monday_date = raw_date - timedelta(days=days_since_monday)
                    if monday_date not in aligned_dates:
                        aligned_dates.append(monday_date)
                elif grain == Granularity.MONTH:
                    # Ensure month dates are always first of month
                    first_of_month = raw_date.replace(day=1)
                    if first_of_month not in aligned_dates:
                        aligned_dates.append(first_of_month)
                else:
                    # For DAY grain, use as-is
                    aligned_dates.append(raw_date)

            grain_dates[grain] = sorted(aligned_dates)
            logger.info(
                f"Date range mode: Generated {len(grain_dates[grain])} aligned dates for {grain} from {start_date}"
                f" to {end_date}"
            )
        else:
            # When not using date range, only use today if it matches granularity requirements
            if grain == Granularity.DAY:
                grain_dates[grain] = [today]
                logger.info(f"Single date mode: Using today ({today}) for {grain}")
            elif grain == Granularity.WEEK and today.weekday() == 0:  # Monday
                grain_dates[grain] = [today]
                logger.info(f"Single date mode: Using today ({today}) for {grain} (Monday)")
            elif grain == Granularity.MONTH and today.day == 1:  # First day of month
                grain_dates[grain] = [today]
                logger.info(f"Single date mode: Using today ({today}) for {grain} (1st of month)")
            else:
                grain_dates[grain] = []
                logger.info(
                    f"Single date mode: No applicable dates for {grain} (today is {today}, weekday={today.weekday()},"
                    f" day={today.day})"
                )

    # Create V2 mock story service with database session
    mock_story_service = MockStoryServiceV2(db_session=db_session)

    # Step 1: Generate all stories first
    all_stories = []

    for pattern in patterns:
        for grain in grains:
            dates = grain_dates[grain]

            if not dates:
                logger.info(f"No applicable dates for pattern {pattern}, grain {grain}")
                continue

            for story_date in dates:
                try:
                    logger.info(f"Generating stories for pattern {pattern}, grain {grain}, date {story_date}")

                    # Generate stories for this specific pattern
                    stories = await mock_story_service.generate_pattern_stories(
                        pattern_name=pattern, metric=metric, grain=grain, story_date=story_date
                    )

                    if stories:
                        all_stories.extend(stories)
                        logger.info(
                            f"Generated {len(stories)} stories for pattern {pattern}, grain {grain}, "
                            f"date {story_date}"
                        )
                    else:
                        logger.warning(f"No stories generated for pattern {pattern}, grain {grain}, date {story_date}")

                except Exception as e:
                    logger.error(
                        f"Error generating stories for pattern {pattern}, grain {grain}, "
                        f"date {story_date}: {str(e)}"
                    )

    logger.info(f"Step 1 complete: Generated total of {len(all_stories)} stories")
    # Step 2: Persist all generated stories
    if all_stories:
        try:
            logger.info(f"Step 2: Persisting {len(all_stories)} stories to database...")
            persisted_stories = await mock_story_service.persist_stories(all_stories)
            logger.info(f"✅ Successfully persisted {len(persisted_stories)} v2 stories to database")
        except Exception as e:
            logger.error(f"Error persisting stories: {str(e)}")
            raise
    else:
        logger.warning("No stories were generated to persist")


async def main(
    tenant_id: int,
    metric_id: str,
    pattern: str | None = None,
    grain: str | None = None,
    start_date_str: str | None = None,
    end_date_str: str | None = None,
) -> None:
    """
    Main function to run the v2 mock story loading process.

    Args:
        tenant_id: The tenant ID
        metric_id: Metric ID to generate stories for
        pattern: Optional comma-separated list of pattern names
        grain: Optional comma-separated list of granularity
        start_date_str: Optional start date in YYYY-MM-DD format
        end_date_str: Optional end date in YYYY-MM-DD format
    """
    # Set tenant context
    logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
    set_tenant_id(tenant_id)

    query_client = await get_query_manager_client()
    # Get metric details from API instead of creating manually
    metric = await query_client.get_metric(metric_id)

    if not metric:
        raise ValueError(f"Metric with ID {metric_id} not found")

    # Validate pattern names
    valid_patterns = ["performance_status", "historical_performance", "dimension_analysis"]
    if pattern not in valid_patterns:
        raise ValueError(f"Invalid pattern: {pattern}. Valid patterns: {valid_patterns}")

    # Parse grain if provided
    if grain:
        try:
            grain = Granularity[grain.strip().upper()]
        except KeyError as e:
            logger.error(f"Invalid granularity: {e}")
            raise ValueError(f"Invalid granularity: {e}") from e

    # Parse start date if provided
    parsed_start_date = None
    if start_date_str:
        try:
            parsed_start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        except ValueError as e:
            logger.error(f"Invalid start date format: {e}")
            raise ValueError("Invalid start date format. Use YYYY-MM-DD.") from e

    # Parse end date if provided
    parsed_end_date = None
    if end_date_str:
        try:
            parsed_end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError as e:
            logger.error(f"Invalid end date format: {e}")
            raise ValueError("Invalid end date format. Use YYYY-MM-DD.") from e

    # Check date constraints
    if parsed_end_date and not parsed_start_date:
        raise ValueError("start_date must be provided when end_date is provided")

    # Load v2 mock stories
    async with open_async_session("story_mock_stories_v2_loader") as session:
        await load_mock_stories_v2(
            db_session=session,
            tenant_id=tenant_id,
            metric=metric,
            pattern=pattern,
            grain=grain,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

    # Clean up
    reset_context()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load v2 mock stories for a specific tenant.")
    parser.add_argument("tenant_id", type=int, help="The tenant ID")
    parser.add_argument("metric_id", type=str, help="Metric ID to generate stories for")
    parser.add_argument(
        "--patterns",
        type=str,
        help="Comma-separated list of patterns (e.g., performance_status,historical_performance,dimension_analysis)",
    )
    parser.add_argument("--grains", type=str, help="Comma-separated list of granularities (e.g., DAY,WEEK,MONTH)")
    parser.add_argument("--start-date", type=str, help="Start date for story generation in YYYY-MM-DD format")
    parser.add_argument("--end-date", type=str, help="End date for story generation in YYYY-MM-DD format")

    args = parser.parse_args()

    asyncio.run(
        main(
            tenant_id=args.tenant_id,
            metric_id=args.metric_id,
            pattern=args.patterns,
            grain=args.grains,
            start_date_str=args.start_date,
            end_date_str=args.end_date,
        )
    )
