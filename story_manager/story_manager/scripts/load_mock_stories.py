import argparse
import asyncio
import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from commons.db.v2 import dispose_session_manager, init_session_manager
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.grain_utils import GrainPeriodCalculator
from story_manager.config import get_settings
from story_manager.core.dependencies import get_query_manager_client
from story_manager.core.enums import StoryGroup
from story_manager.mocks.services.story_loader import MockStoryLoader

# Set up logging
logger = logging.getLogger(__name__)

# get settings
settings = get_settings()
# initialize session manager
session_manager = init_session_manager(settings, app_name="story_mock_stories_loader")


async def load_mock_stories(
    db_session: AsyncSession,
    tenant_id: int,
    metric: dict[str, Any],
    story_groups: list[StoryGroup] | None = None,
    grains: list[Granularity] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """
    Load v1 mock stories into the database for given parameters using story groups.

    :param db_session: Database session
    :param tenant_id: Tenant ID
    :param metric: Metric dictionary containing metric_id
    :param story_groups: List of story groups to generate, defaults to all
    :param grains: List of granularity to generate, defaults to all
    :param start_date: Optional start date for story generation
    :param end_date: Optional end date for story generation, defaults to today if start_date is provided
    :return: List of created Story objects
    """
    logger.info(f"Loading v1 mock stories for tenant {tenant_id}, metric {metric['metric_id']}")

    # Default to all story groups if not specified
    if not story_groups:
        story_groups = list(StoryGroup)

    # Default to all granularity if not specified
    if not grains:
        grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    # Handle date logic
    today = date.today()

    # Validate date parameters
    if end_date is not None and start_date is None:
        raise ValueError("start_date must be provided when end_date is provided")

    if start_date is not None and end_date is None:
        end_date = today

    # Prepare a dictionary to hold dates for each granularity
    grain_dates = {}

    # Pre-compute dates for each granularity outside the loops
    for grain in grains:
        if start_date is not None:
            # When using date range, get all dates within the range
            grain_dates[grain] = GrainPeriodCalculator.get_dates_for_range(
                grain, start_date=start_date, end_date=end_date  # type: ignore
            )
        else:
            # When not using date range, only use today if it matches granularity requirements
            if grain == Granularity.DAY:
                grain_dates[grain] = [today]
            elif grain == Granularity.WEEK and today.weekday() == 0:  # Monday
                grain_dates[grain] = [today]
            elif grain == Granularity.MONTH and today.day == 1:  # First day of month
                grain_dates[grain] = [today]
            else:
                grain_dates[grain] = []

    # V1 story generation using story groups
    loader = MockStoryLoader(db_session)  # type: ignore

    for story_group in story_groups:
        for grain in grains:
            dates = grain_dates[grain]

            if not dates:
                logger.info(f"No applicable dates for {story_group}, {grain}")
                continue

            for story_date in dates:
                try:
                    # Generate the stories
                    stories = loader.prepare_stories(
                        metric=metric, grain=grain, story_group=story_group, story_date=story_date  # type: ignore
                    )
                    if stories:
                        await loader.persist_stories(stories)
                        logger.info(
                            f"Generated and stored {len(stories)} v1 stories for {story_group}, {grain}, "
                            f"{story_date}"
                        )
                except Exception as e:
                    logger.error(f"Error generating v1 stories for {story_group}, {grain}, {story_date}: {str(e)}")


async def main(
    tenant_id: int,
    metric_id: str,
    story_groups: str | None = None,
    grains: str | None = None,
    start_date_str: str | None = None,
    end_date_str: str | None = None,
) -> None:
    """
    Main function to run the v1 mock story loading process.

    :param tenant_id: The tenant ID
    :param metric_id: Metric ID to generate stories for
    :param story_groups: Optional comma-separated list of story groups
    :param grains: Optional comma-separated list of granularity
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

    try:
        # Load v1 mock stories
        async with session_manager.session() as session:
            await load_mock_stories(
                db_session=session,
                tenant_id=tenant_id,
                metric=metric,
                story_groups=parsed_story_groups,
                grains=parsed_grains,
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
    except Exception as e:
        logger.error(f"Error loading v1 mock stories: {str(e)}")
        raise
    finally:
        # Clean up
        reset_context()
        logger.info("Disposing AsyncSessionManager")
        await dispose_session_manager()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load v1 mock stories for a specific tenant.")
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
