import os
from datetime import date
from typing import Any

from prefect import get_run_logger, task

from commons.db.v2 import async_session
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from levers import Levers
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager
from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.story_evaluator.manager import StoryEvaluatorManager
from tasks_manager.config import AppConfig, get_settings
from tasks_manager.services.pattern_data_organiser import PatternDataOrganiser
from tasks_manager.tasks.pattern_analysis import get_pattern_config
from tasks_manager.tasks.query import get_metric
from tasks_manager.utils import increment_date_by_grain, should_update_grain


@task(
    retries=3,
    retry_delay_seconds=60,
    tags=["story-update", "db-operation"],
    task_run_name="update_demo_tenant_stories:{tenant_id}",
)
async def update_demo_tenant_stories(tenant_id: int, current_date: date) -> dict[str, Any]:
    """
    Update story dates for a specific tenant

    :param tenant_id: The tenant ID
    :param current_date: The current date to check for updates
    :return: Dictionary with update results
    """
    logger = get_run_logger()
    config = await AppConfig.load("default")
    # Setup tenant context
    set_tenant_id(tenant_id)
    os.environ["SERVER_HOST"] = config.story_manager_server_host

    logger.info(f"Updating story dates for tenant {tenant_id}")

    updates_by_grain = {
        Granularity.DAY: 0,
        Granularity.WEEK: 0,
        Granularity.MONTH: 0,
        Granularity.QUARTER: 0,
        Granularity.YEAR: 0,
    }

    stories_updated = 0

    async with async_session(get_settings(), app_name="tasks_manager") as session:
        # Get all stories for this tenant
        story_crud = CRUDStory(model=Story, session=session)
        stories = await story_crud.get_stories()

        logger.info(f"Retrieved {len(stories)} stories for tenant {tenant_id}")

        for story in stories:
            grain = Granularity(story.grain)
            story_date = story.story_date
            story_id = story.id

            # Check if we should update this grain today
            if should_update_grain(current_date, grain):
                # Get a new story date by incrementing by one grain
                new_story_date = increment_date_by_grain(story_date, grain)
                # Update the story with the new date
                updated = await story_crud.update_story_date(story_id=story_id, new_date=new_story_date)
                if updated:
                    updates_by_grain[grain] += 1
                    stories_updated += 1

    # # Clean up tenant context
    reset_context()

    logger.info(f"Updated {stories_updated} stories for tenant {tenant_id}")
    logger.info(
        f"Updates by grain: Day: {updates_by_grain[Granularity.DAY]}, "
        f"Week: {updates_by_grain[Granularity.WEEK]}, "
        f"Month: {updates_by_grain[Granularity.MONTH]}, "
        f"Quarter: {updates_by_grain[Granularity.QUARTER]}, "
        f"Year: {updates_by_grain[Granularity.YEAR]}"
    )

    return {"tenant_id": tenant_id, "stories_updated": stories_updated, "updates_by_grain": updates_by_grain}


@task(
    retries=2,
    retry_delay_seconds=30,
    tags=["story-generation", "db-operation"],
    task_run_name="process_pattern_stories:{pattern}_metric:{metric_id}_tenant:{tenant_id}_grain:{grain}",
)
async def process_pattern_stories(
    pattern: str, tenant_id: int, metric_id: str, grain: Granularity, pattern_run: dict
) -> list[dict]:
    """
    Process pattern results and generate stories.

    Args:
        pattern: Pattern name
        tenant_id: Tenant ID
        metric_id: Metric ID
        grain: Granularity
        pattern_run: Pattern run results

    Returns:
        List of generated stories
    """
    logger = get_run_logger()
    logger.info(
        "Processing pattern stories for tenant %s, pattern %s, metric %s, grain %s",
        tenant_id,
        pattern,
        metric_id,
        grain.value,
    )

    # Setup tenant context
    set_tenant_id(tenant_id)
    config = await AppConfig.load("default")
    os.environ["SERVER_HOST"] = config.story_manager_server_host

    try:
        # Load pattern data as the appropriate pattern model
        pattern_run_obj = Levers.load_pattern_model(pattern_run)

        # Get metric details
        metric: MetricDetail = await get_metric(metric_id)

        # Get pattern config to fetch series data
        pattern_config = await get_pattern_config(pattern)

        # Initialize story evaluator manager
        manager = StoryEvaluatorManager()

        # Use a single database session for both data fetching and story persistence
        async with async_session(get_settings(), app_name="tasks_manager") as session:
            # Fetch series data using PatternDataOrganiser
            semantic_manager = SemanticManager(session)
            data_organiser = PatternDataOrganiser(semantic_manager=semantic_manager)
            series_data = await data_organiser.fetch_data_for_pattern(
                config=pattern_config,
                metric_id=metric_id,
                grain=grain,  # type: ignore
                metric_definition=metric,
            )
            # Get the time series data from the fetched data
            data_key = pattern_config.data_sources[0].data_key
            # todo: Add multiple data sources handling once we have more than one data source for a pattern
            series_df = series_data[data_key] if series_data else None

            # Process pattern results and generate stories with series data
            stories = await manager.evaluate_pattern_result(pattern_run_obj, metric.model_dump(), series_df)
            logger.info(
                "Generated %d stories for pattern %s, metric %s, grain %s",
                len(stories),
                pattern,
                metric_id,
                grain.value,
            )

            # Persist stories using the same session
            story_objs = await manager.persist_stories(stories, session)
            logger.info(
                "Persisted %d stories for pattern %s, metric %s, grain %s",
                len(stories),
                pattern,
                metric_id,
                grain.value,
            )

            # Return stories as dictionaries for the event payload
            story_dicts = [story.model_dump(mode="json") for story in story_objs]
            # Need to keep these keys in the story Artifact
            artifact_keys = [
                "id",
                "genre",
                "story_group",
                "story_type",
                "grain",
                "metric_id",
                "title",
                "detail",
                "story_date",
                "is_heuristic",
                "pattern_run_id",
            ]
            # Filter story dicts to keep only the keys we want and heuristic stories
            story_records = [{key: story[key] for key in artifact_keys} for story in story_dicts]
            return story_records

    except Exception as e:
        logger.error(
            "Error processing pattern stories for tenant %s, pattern %s, metric %s, grain %s: %s",
            tenant_id,
            pattern,
            metric_id,
            grain.value,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        # Reset tenant context
        reset_context()
