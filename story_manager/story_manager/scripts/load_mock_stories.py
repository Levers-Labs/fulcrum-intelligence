import argparse
import asyncio
import logging
from datetime import date, datetime
from typing import Any

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.dependencies import QueryManagerClientDep, get_query_manager_client
from story_manager.core.enums import StoryGroup
from story_manager.core.models import Story
from story_manager.db.config import get_async_session
from story_manager.mocks.services.data_service import MockDataService
from story_manager.mocks.services.story_loader import MockStoryLoader

# Set up logging
logger = logging.getLogger(__name__)


async def load_mock_stories(
    tenant_id: int,
    metric: dict[str, Any],
    story_groups: list[StoryGroup] = None,
    grains: list[Granularity] = None,
    start_date: date = None,
    end_date: date = None,
) -> list[Story]:
    """
    Load mock stories into the database for given parameters.

    :param query_manager:
    :param tenant_id: Tenant ID
    :param metric_id: Metric ID to generate stories for
    :param story_groups: List of story groups to generate, defaults to all
    :param grains: List of granularities to generate, defaults to all
    :param start_date: Optional start date for story generation
    :param end_date: Optional end date for story generation, defaults to today if start_date is provided
    :return: List of created Story objects
    """
    logger.info(f"Loading mock stories for tenant {tenant_id}, metric {metric['metric_id']}")

    # Default to all story groups if not specified
    if not story_groups:
        story_groups = list(StoryGroup)

    # Default to all granularities if not specified
    if not grains:
        grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    # Handle date logic
    today = date.today()
    using_date_range = start_date is not None

    # If only end_date is provided without start_date, raise error
    if start_date is None and end_date is not None:
        raise ValueError("start_date must be provided when end_date is provided")

    # If start_date is provided but end_date is not, default end_date to today
    if start_date is not None and end_date is None:
        end_date = today

    # If neither is provided, we'll use today's date with granularity filtering
    if start_date is None and end_date is None:
        end_date = today

    all_stories = []

    async with get_async_session() as db_session:

        # Create the loader
        loader = MockStoryLoader(db_session)
        mock_data_service = MockDataService()

        # Generate and load stories for each combination
        for story_group in story_groups:
            for grain in grains:
                if using_date_range:
                    # When using date range, get all dates within the range
                    dates = mock_data_service.get_dates_for_range(grain, start_date=start_date, end_date=end_date)
                else:
                    # When not using date range, only use today if it matches granularity requirements
                    dates = []
                    # Only add today if it meets granularity requirements
                    if grain == Granularity.DAY:
                        dates = [today]
                    elif grain == Granularity.WEEK and today.weekday() == 0:  # Monday
                        dates = [today]
                    elif grain == Granularity.MONTH and today.day == 1:  # First day of month
                        dates = [today]

                for story_date in dates:
                    try:
                        stories = await loader.run(
                            metric=metric, grain=grain, story_group=story_group, story_date=story_date
                        )
                        all_stories.extend(stories)
                        logger.info(
                            f"Generated {len(stories)} stories for {story_group}, {grain}, {story_date}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error generating stories for {story_group}, {grain}, {story_date}: {str(e)}"
                        )

    logger.info(f"Successfully loaded {len(all_stories)} mock stories")
    return all_stories


async def main(
    tenant_id: int,
    metric_id: str,
    story_groups: str | None = None,
    grains: str | None = None,
    start_date_str: str | None = None,
    end_date_str: str | None = None,
) -> None:
    """
    Main function to run the mock story loading process.

    :param tenant_id: The tenant ID
    :param metric_id: Metric ID to generate stories for
    :param story_groups: Optional comma-separated list of story groups
    :param grains: Optional comma-separated list of granularities
    :param start_date_str: Optional start date in YYYY-MM-DD format
    :param end_date_str: Optional end date in YYYY-MM-DD format
    """
    # Set tenant context
    logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
    set_tenant_id(tenant_id)

    query_client = await get_query_manager_client()
    # Get metric details from API instead of creating manually
    metric = await query_client.get_metric(metric_id)

    if not metric:
        raise ValueError(f"Metric with ID {metric_id} not found")

    # Parse story groups if provided
    parsed_story_groups = None
    if story_groups:
        try:
            parsed_story_groups = [StoryGroup[sg.strip().upper()] for sg in story_groups.split(",")]
        except KeyError as e:
            logger.error(f"Invalid story group: {e}")
            raise ValueError(f"Invalid story group: {e}") from e

    # Parse grains if provided
    parsed_grains = None
    if grains:
        try:
            parsed_grains = [Granularity[g.strip().upper()] for g in grains.split(",")]
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

    # Load mock stories
    await load_mock_stories(
        tenant_id=tenant_id,
        metric=metric,
        story_groups=parsed_story_groups,
        grains=parsed_grains,
        start_date=parsed_start_date,
        end_date=parsed_end_date,
    )

    # Clean up
    reset_context()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load mock stories for a specific tenant.")
    parser.add_argument("tenant_id", type=int, help="The tenant ID")
    parser.add_argument("metric_id", type=str, help="Metric ID to generate stories for")
    parser.add_argument(
        "--story-groups", type=str, help="Comma-separated list of story groups (e.g., LONG_RANGE,GOAL_VS_ACTUAL)"
    )
    parser.add_argument("--grains", type=str, help="Comma-separated list of granularities (e.g., DAY,WEEK,MONTH)")
    parser.add_argument("--start-date", type=str, help="Start date for story generation in YYYY-MM-DD format")
    parser.add_argument("--end-date", type=str, help="End date for story generation in YYYY-MM-DD format")

    args = parser.parse_args()

    asyncio.run(
        main(
            tenant_id=args.tenant_id,
            metric_id=args.metric_id,
            story_groups=args.story_groups,
            grains=args.grains,
            start_date_str=args.start_date,
            end_date_str=args.end_date,
        )
    )
