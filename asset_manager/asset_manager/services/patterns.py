"""Pattern analysis orchestration service.

Provides functional orchestration for pattern analysis, mirroring the Prefect
flow logic but adapted for Dagster assets with RORO pattern and async I/O.
"""

from datetime import date
from typing import Any

from analysis_manager.patterns.manager import PatternManager
from asset_manager.resources.db import DbResource
from asset_manager.services.pattern_data_organiser import DataDict, PatternDataOrganiser
from asset_manager.services.utils import get_metric
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from levers import DataError, Levers, PatternError
from levers.models.common import AnalysisWindow
from levers.models.pattern_config import PatternConfig
from query_manager.core.schemas import MetricDetail
from query_manager.semantic_manager.crud import SemanticManager


async def get_pattern_config(db: DbResource, pattern: str) -> PatternConfig:
    """Get pattern configuration from analysis manager."""
    async with db.session() as session:
        pm = PatternManager(session)
        return await pm.get_pattern_config(pattern)


def build_analysis_window(config: PatternConfig, grain: Granularity, analysis_date: date) -> AnalysisWindow:
    """Build analysis window from pattern config and grain."""
    start_date, end_date = config.analysis_window.get_date_range(grain=grain, analysis_date=analysis_date)  # type: ignore
    return AnalysisWindow(start_date=start_date.isoformat(), end_date=end_date.isoformat(), grain=grain)  # type: ignore


async def fetch_pattern_data(
    db: DbResource,
    pattern_config: PatternConfig,
    grain: Granularity,
    metric: MetricDetail,
    analysis_date: date,
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
            analysis_date=analysis_date,
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
    sync_date: date,
    logger: Any,
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
            analysis_date=sync_date,
            dimension_name=dimension_name,
        )
        # Build analysis window
        analysis_window = build_analysis_window(pattern_config, grain, sync_date)

        # Prepare arguments for pattern execution
        kwargs: dict[str, Any] = {
            "metric_id": metric_id,
            "analysis_date": sync_date,
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

        error = stored.get("error")
        if error:
            error_message = error.get("message", "Unknown error")
            error_type = error.get("type", "Unknown error")
            if error_type == "data_error":
                raise DataError(error_message)
            raise PatternError(
                message=error_message,
                pattern_name=pattern,
                details=error,
            )

        return {
            "success": True,
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
    sync_date: date,
    pattern: str,
    logger: Any,
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
        failed_dimensions: list[dict[str, Any]] = []

        # Standard patterns (metric-level)
        if not is_dimension_pattern:
            result = await run_single_pattern(
                db, pattern_config=pattern_config, grain=grain, metric=metric, sync_date=sync_date, logger=logger
            )
            runs_results.append(result)

        # Dimension patterns (per dimension)
        if is_dimension_pattern and metric.dimensions:
            dimensions = [d.dimension_id for d in metric.dimensions]

            for dimension in dimensions:
                try:
                    result = await run_single_pattern(
                        db,
                        pattern_config=pattern_config,
                        grain=grain,
                        metric=metric,
                        dimension_name=dimension,
                        sync_date=sync_date,
                        logger=logger,
                    )
                    runs_results.append(result)
                except Exception as e:
                    logger.warning(
                        "Failed to process dimension %s for pattern %s on metric %s: %s",
                        dimension,
                        pattern,
                        metric_id,
                        str(e),
                    )
                    failed_dimensions.append(
                        {
                            "dimension": dimension,
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "pattern": pattern,
                            "metric_id": metric_id,
                            "grain": grain.value,
                            "sync_date": sync_date.isoformat(),
                        }
                    )

            # Log summary of dimension processing
            if failed_dimensions:
                total_dimensions = len(dimensions)
                successful_dimensions = total_dimensions - len(failed_dimensions)
                logger.info(
                    "Dimension pattern processing summary for %s: %d/%d dimensions processed successfully, %d failed",
                    metric_id,
                    successful_dimensions,
                    total_dimensions,
                    len(failed_dimensions),
                )

        # Summarize results
        successes = [r for r in runs_results if r.get("success")]
        failures = [r for r in runs_results if not r.get("success")]
        pattern_names = list({r.get("pattern") for r in runs_results if r.get("pattern")})

        # Handle dimensional pattern results and metadata
        metadata: dict[str, Any] = {}
        if is_dimension_pattern and metric.dimensions:
            total_dimensions = len([d.dimension_id for d in metric.dimensions])

            # Fail entire run if all dimensions failed
            if len(failed_dimensions) == total_dimensions:
                logger.error(
                    "All %d dimensions failed for pattern %s on metric %s. Failing entire run.",
                    total_dimensions,
                    pattern,
                    metric_id,
                )
                raise PatternError(
                    message=f"All {total_dimensions} dimensions failed for pattern {pattern} on metric {metric_id}",
                    pattern_name=pattern,
                    details={
                        "metric_id": metric_id,
                        "pattern": pattern,
                        "grain": grain.value,
                        "sync_date": sync_date.isoformat(),
                        "total_dimensions": total_dimensions,
                        "failed_dimensions": failed_dimensions,
                    },
                )

            # Include failed dimensions in metadata if any
            if failed_dimensions:
                metadata["failed_dimensions"] = failed_dimensions
            metadata["total_dimensions"] = total_dimensions
            metadata["processed_dimensions"] = len(runs_results)
            metadata["failed_dimension_count"] = len(failed_dimensions)

        result = {
            "executed": len(runs_results),
            "successes": len(successes),
            "failed": len(failures),
            "patterns": pattern_names,
            "status": "success" if not failures else "partial_success" if successes else "failed",
            "runs": runs_results,
        }

        # Add metadata if present
        if metadata:
            result = {**result, **metadata}

        return result
    finally:
        reset_context()
