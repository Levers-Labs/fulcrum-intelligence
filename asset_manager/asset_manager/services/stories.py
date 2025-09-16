"""Story generation service for processing pattern results.

Provides functional service for generating stories from pattern analysis results,
mirroring the Prefect task logic but adapted for Dagster assets with RORO pattern.
"""

import logging
from datetime import date
from typing import Any

from asset_manager.resources.db import DbResource
from asset_manager.services.pattern_data_organiser import PatternDataOrganiser
from asset_manager.services.patterns import get_pattern_config
from asset_manager.services.utils import get_metric
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from levers import Levers
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager
from story_manager.story_evaluator.manager import StoryEvaluatorManager

logger = logging.getLogger(__name__)

STORY_METADATA_KEYS = ["id", "genre", "story_group", "story_type", "title", "dimension_name", "is_heuristic"]


async def generate_stories_for_pattern_run(
    *,
    tenant_id: int,
    db: DbResource,
    metric_id: str,
    pattern: str,
    grain: Granularity,
    analysis_date: date,
    run: dict,
) -> list[dict[str, Any]]:
    """
    Generate stories for a single pattern run result.
    Returns:
        List of dictionaries with story metadata
    """
    # Setup tenant context
    set_tenant_id(tenant_id)
    try:
        logger.info(
            "Processing pattern stories for tenant %s, pattern %s, metric %s, grain %s, date %s",
            tenant_id,
            pattern,
            metric_id,
            grain.value,
            analysis_date,
        )

        # Load pattern data as the appropriate pattern model
        run_result = run.get("run_result", run)  # Handle both formats
        pattern_run_obj = Levers.load_pattern_model(run_result)

        # Get metric details
        async with db.session() as session:
            metric: MetricDetail = await get_metric(metric_id, session)

        # Get pattern config to fetch series data
        pattern_config = await get_pattern_config(db, pattern)

        # Initialize story evaluator manager
        manager = StoryEvaluatorManager()

        # Fetch series data using a dedicated micro session
        series_df = None
        async with db.session() as fetch_session:
            semantic_manager = SemanticManager(fetch_session)
            data_organiser = PatternDataOrganiser(semantic_manager=semantic_manager)
            series_data = await data_organiser.fetch_data_for_pattern(
                config=pattern_config,
                metric_id=metric_id,
                grain=grain,  # type: ignore
                metric_definition=metric,
                analysis_date=analysis_date,
            )

            # Get the time series data from the fetched data
            data_key = pattern_config.data_sources[0].data_key
            # TODO: Add multiple data sources handling once we have more than one data source for a pattern
            series_df = series_data[data_key] if series_data else None

        # Process pattern results and generate stories (CPU-bound, no session needed)
        stories = await manager.evaluate_pattern_result(pattern_run_obj, metric.model_dump(), series_df)
        logger.info(
            "Generated %d stories for pattern %s, metric %s, grain %s, date %s",
            len(stories),
            pattern,
            metric_id,
            grain.value,
            analysis_date,
        )

        # Persist stories using a dedicated micro session for writes
        async with db.session() as persist_session:
            story_objs = await manager.persist_stories(stories, persist_session)
            logger.info(
                "Persisted %d stories for pattern %s, metric %s, grain %s, date %s",
                len(story_objs),
                pattern,
                metric_id,
                grain.value,
                analysis_date,
            )
            story_dicts = [story.model_dump(mode="json") for story in story_objs]
        # Filter story dicts to keep only the keys we want and heuristic stories
        story_records = [{key: story[key] for key in STORY_METADATA_KEYS} for story in story_dicts]

        return story_records

    except Exception as e:
        logger.error(
            "Error processing pattern stories for tenant %s, pattern %s, metric %s, grain %s, date %s: %s",
            tenant_id,
            pattern,
            metric_id,
            grain.value,
            analysis_date,
            str(e),
            exc_info=True,
        )
        raise
    finally:
        # Reset tenant context
        reset_context()


async def generate_stories_for_pattern_runs(
    *,
    tenant_id: int,
    db: DbResource,
    metric_id: str,
    pattern: str,
    grain: Granularity,
    analysis_date: date,
    runs: list[dict],
) -> dict[str, Any]:
    """
    Generate stories for multiple pattern runs, processing each run individually.
    Returns:
        Dictionary with aggregated story count, IDs, and processing metadata
    """
    logger.info(
        "Processing %d pattern runs for tenant %s, pattern %s, metric %s, grain %s, date %s",
        len(runs),
        tenant_id,
        pattern,
        metric_id,
        grain.value,
        analysis_date,
    )

    # Process each pattern run individually
    stories: list[dict[str, Any]] = []

    for i, run in enumerate(runs):
        logger.debug(f"Processing pattern run {i+1}/{len(runs)} for {pattern}::{metric_id}")
        result = await generate_stories_for_pattern_run(
            tenant_id=tenant_id,
            db=db,
            metric_id=metric_id,
            pattern=pattern,
            grain=grain,
            analysis_date=analysis_date,
            run=run,
        )

        stories.extend(result)
    logger.info(
        "Generated %d total stories from %d pattern runs for tenant %s, pattern %s, metric %s, grain %s",
        len(stories),
        len(runs),
        tenant_id,
        pattern,
        metric_id,
        grain.value,
    )

    return {
        "count": len(stories),
        "stories": stories,
        "metric_id": metric_id,
        "pattern": pattern,
        "grain": grain.value,
        "tenant_id": tenant_id,
    }
