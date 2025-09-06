"""Pattern analysis orchestration service.

Provides functional orchestration for pattern analysis, mirroring the Prefect
flow logic but adapted for Dagster assets with RORO pattern and async I/O.
"""

import logging
from typing import Any

from analysis_manager.patterns.manager import PatternManager
from asset_manager.resources.db import DbResource
from asset_manager.services.pattern_data_organiser import DataDict, PatternDataOrganiser
from asset_manager.services.utils import get_metric
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from levers import Levers
from levers.models.common import AnalysisWindow
from levers.models.pattern_config import PatternConfig
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager

logger = logging.getLogger(__name__)


async def get_pattern_config(db: DbResource, pattern: str) -> PatternConfig:
    """Get pattern configuration from analysis manager."""
    async with db.session() as session:
        pm = PatternManager(session)
        return await pm.get_pattern_config(pattern)


def build_analysis_window(config: PatternConfig, grain: Granularity) -> AnalysisWindow:
    """Build analysis window from pattern config and grain."""
    start_date, end_date = config.analysis_window.get_date_range(grain=grain)  # type: ignore
    return AnalysisWindow(start_date=start_date.isoformat(), end_date=end_date.isoformat(), grain=grain)  # type: ignore


async def fetch_pattern_data(
    db: DbResource,
    pattern_config: PatternConfig,
    grain: Granularity,
    metric: MetricDetail,
    dimension_name: str | None = None,
) -> DataDict:
    """Fetch data required for pattern analysis."""
    async with db.session() as session:
        organiser = PatternDataOrganiser(semantic_manager=SemanticManager(session))
        kwargs: dict[str, Any] = {}
        if dimension_name:
            kwargs["dimension_name"] = dimension_name
        return await organiser.fetch_data_for_pattern(
            config=pattern_config,
            metric_id=metric.metric_id,
            grain=grain,  # type: ignore
            metric_definition=metric,
            **kwargs,
        )


async def store_pattern_result(db: DbResource, result: Any) -> dict[str, Any]:
    """Store pattern analysis result in the analysis store."""
    async with db.session() as session:
        pm = PatternManager(session)
        stored = await pm.store_pattern_result(pattern_name=result.pattern, pattern_result=result)
        return stored.model_dump(mode="json")


async def run_single_pattern(
    db: DbResource,
    pattern_config: PatternConfig,
    grain: Granularity,
    metric: MetricDetail,
    dimension_name: str | None = None,
) -> dict[str, Any]:
    """Run a single pattern analysis."""
    levers: Levers = Levers()
    pattern = pattern_config.pattern_name
    metric_id = metric.metric_id

    try:
        # Fetch pattern data
        data = await fetch_pattern_data(
            db=db,
            pattern_config=pattern_config,
            grain=grain,
            metric=metric,
            dimension_name=dimension_name,
        )

        # Check if any required dataset is empty
        for key, df in data.items():
            if df.empty:
                return {
                    "success": False,
                    "pattern": pattern,
                    "metric_id": metric_id,
                    "grain": grain.value,
                    "error": {"message": f"No data available for {key}", "type": "NoDataAvailableError"},
                    **({"dimension_name": dimension_name} if dimension_name else {}),
                }

        # Build analysis window
        analysis_window = build_analysis_window(pattern_config, grain)

        # Prepare arguments for pattern execution
        kwargs: dict[str, Any] = {
            "metric_id": metric_id,
            **({"dimension_name": dimension_name} if dimension_name else {}),
        }
        for key, frame in data.items():
            kwargs[key] = frame

        # Execute pattern
        result = levers.execute_pattern(
            pattern_name=pattern, analysis_window=analysis_window, config=pattern_config, **kwargs
        )

        # Store result
        stored = await store_pattern_result(db, result=result)

        return {
            "success": False if stored.get("error") else True,
            **stored,
        }

    except Exception as e:
        logger.error("Error running pattern %s for metric %s on %s: %s", pattern, metric_id, grain.value, e)
        raise


async def run_patterns_for_metric(
    tenant_id: int,
    db: DbResource,
    metric_id: str,
    grain: Granularity,
    pattern: str,
) -> dict[str, Any]:
    """
    Run pattern analysis for a metric, handling both standard and dimension-based patterns for a given grain.
    """
    try:
        # Set tenant context
        set_tenant_id(tenant_id)
        # Get metric definition
        async with db.session() as session:
            metric = await get_metric(metric_id, session)
        # Get pattern configuration
        pattern_config = await get_pattern_config(db, pattern)
        is_dimension_pattern = pattern_config.needs_dimension_analysis

        # Runs results
        runs_results: list[dict[str, Any]] = []
        # Standard patterns (metric-level)
        if not is_dimension_pattern:
            result = await run_single_pattern(db, pattern_config=pattern_config, grain=grain, metric=metric)
            runs_results.append(result)

        # Dimension patterns (per dimension)
        if is_dimension_pattern and metric.dimensions:
            dimensions = [d.dimension_id for d in metric.dimensions]
            for dimension in dimensions:
                result = await run_single_pattern(
                    db, pattern_config=pattern_config, grain=grain, metric=metric, dimension_name=dimension
                )
                runs_results.append(result)

        # Summarize results
        successes = [r for r in runs_results if r.get("success")]
        failures = [r for r in runs_results if not r.get("success")]
        pattern_names = list({r.get("pattern") for r in runs_results if r.get("pattern")})

        return {
            "executed": len(runs_results),
            "successes": len(successes),
            "failed": len(failures),
            "patterns": pattern_names,
            "status": "success" if not failures else "partial_success" if successes else "failed",
            "runs": runs_results,
        }
    finally:
        reset_context()
