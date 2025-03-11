import asyncio
from datetime import date

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from story_manager.core.enums import StoryGroup
from story_manager.db.config import get_async_session
from story_manager.mocks.services.data_service import MockDataService
from story_manager.mocks.services.story_loader import MockStoryLoader


async def load_mock_stories_example(db_session):
    """Example of loading mock stories"""
    print(f"started")
    # Create the loader
    loader = MockStoryLoader(db_session)

    # Load mock stories for different metrics and story groups
    metrics = [
        "newInqs",
        # "inqToMqlRate",
        # "customer_satisfaction"
    ]

    story_groups = [
        StoryGroup.LONG_RANGE,
        StoryGroup.GOAL_VS_ACTUAL,
        StoryGroup.GROWTH_RATES,
        StoryGroup.RECORD_VALUES,
        StoryGroup.TREND_EXCEPTIONS,
        StoryGroup.TREND_CHANGES,
        StoryGroup.STATUS_CHANGE,
        StoryGroup.REQUIRED_PERFORMANCE,
        StoryGroup.LIKELY_STATUS,
        # StoryGroup.COMPONENT_DRIFT,
        # StoryGroup.INFLUENCE_DRIFT,
        StoryGroup.SEGMENT_DRIFT,
        StoryGroup.SIGNIFICANT_SEGMENTS,
    ]

    all_stories = []

    # Generate and load stories for each combination
    for metric_id in metrics:
        for story_group in story_groups:
            for grain in [Granularity.MONTH, Granularity.DAY, Granularity.WEEK]:
                # start_date, end_date = MockDataService()._get_input_time_range(grain, story_group)
                # dates = MockDataService().get_dates_for_range(grain,
                #                                               start_date=start_date,
                #                                               end_date=date(2025, 3, 10))
                # for sdate in dates:
                stories = await loader.run(
                    metric_id=metric_id, grain=grain, story_group=story_group, story_date=date(2025, 3, 11)
                )
                all_stories.extend(stories)

    print(f"Loaded a total of {len(all_stories)} mock stories")
    return all_stories


async def main(tenant_id: int = 3) -> None:
    """
    Main function to run the upsert process.
    """
    # Set tenant id context in the db session
    # logger.info("Setting tenant context, Tenant ID: %s", tenant_id)
    set_tenant_id(tenant_id)

    async with get_async_session() as db_session:
        await load_mock_stories_example(db_session)
        # Clean up
        # clear context
        # reset_context()


# Usage example:
if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Upsert Story Config for a specific tenant.")
    # parser.add_argument("tenant_id", type=str, help="The tenant ID for which to upsert the story config.")
    # args = parser.parse_args()

    asyncio.run(main())
