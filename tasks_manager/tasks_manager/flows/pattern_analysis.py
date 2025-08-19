from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger, unmapped
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from levers import Levers
from tasks_manager.tasks.pattern_analysis import create_pattern_artifact, get_pattern_config, run_pattern
from tasks_manager.tasks.query import get_metric


@flow(
    name="pattern-analysis-metric",
    flow_run_name="pattern-analysis-metric:tenant={tenant_id_str}_metric={metric_id}_grain={grain}",
    description="Run pattern analysis for a specific metric",
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=3600,  # 1 hour
)
async def analyze_metric_patterns(tenant_id_str: str, metric_id: str, grain: str):
    """
    Run pattern analysis for a specific metric.
    This flow is triggered by the metric.semantic.sync.success event.

    Args:
        tenant_id_str: Tenant ID as string
        metric_id: Metric ID
        grain: Granularity (day, week, month)
    """
    logger = get_run_logger()
    tenant_id = int(tenant_id_str)
    grain = Granularity(grain)

    logger.info("Starting pattern analysis for tenant %s, metric %s, grain %s", tenant_id, metric_id, grain)

    # Set tenant context
    set_tenant_id(tenant_id)

    try:
        # Get all patterns to run using Levers API
        levers: Levers = Levers()

        available_patterns = levers.list_patterns()
        logger.info("Found %s available patterns: %s", len(available_patterns), available_patterns)

        if not available_patterns:
            logger.warning("No patterns available for analysis")
            # Emit the skip event
            emit_event(
                event="metric.pattern.analysis.skipped",
                resource={
                    "prefect.resource.id": f"metric.{metric_id}",
                    "metric_id": metric_id,
                    "tenant_id": tenant_id_str,
                    "grain": grain,
                },
                payload={"status": "skipped", "reason": "no_patterns"},
            )
            return

        # Emit the start event
        emit_event(
            event="metric.pattern.analysis.started",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": tenant_id_str,
                "grain": grain,
            },
            payload={"patterns": available_patterns},
        )

        # Get the metric definition with dimensions
        metric_definition = await get_metric(metric_id=metric_id)

        logger.info(
            "Metric details, name: %s, dimensions count: %s",
            metric_definition.metric_id,
            len(metric_definition.dimensions) if metric_definition.dimensions else 0,
        )

        # Categorize patterns into standard and dimension-based
        standard_patterns = []
        dimension_patterns = []

        # Populate the pattern lists
        for pattern_name in available_patterns:
            # Get the pattern config
            pattern_config = await get_pattern_config(pattern_name)
            # Check if the pattern needs dimension analysis
            if pattern_config.needs_dimension_analysis:
                dimension_patterns.append(pattern_name)
            else:
                standard_patterns.append(pattern_name)

        logger.info("Categorized patterns - Standard: %s, Dimension-based: %s", standard_patterns, dimension_patterns)

        # Prepare the tasks for execution
        pattern_futures: Any = []

        # Process standard patterns
        if standard_patterns:
            logger.info("Submitting tasks for standard patterns: %s", standard_patterns)
            standard_futures = run_pattern.map(
                pattern=standard_patterns,
                metric_id=unmapped(metric_id),
                grain=unmapped(grain),
                metric_definition=unmapped(metric_definition),
            )
            logger.info("Submitted tasks for standard patterns: %s", standard_futures)
            pattern_futures.extend(standard_futures)

        # Process dimension patterns
        if dimension_patterns and metric_definition.dimensions:
            # Get available dimensions from the metric definition
            dimensions = [dim.dimension_id for dim in metric_definition.dimensions]
            logger.info("Found %d dimensions for metric %s: %s", len(dimensions), metric_id, dimensions)

            # For each dimension pattern, create tasks for all dimensions
            for pattern_name in dimension_patterns:
                for dimension_name in dimensions:
                    logger.info("Submitting task for pattern %s and dimension %s", pattern_name, dimension_name)
                    # Create a task for each pattern-dimension combination
                    dimension_future = run_pattern.submit(
                        pattern=pattern_name,
                        metric_id=metric_id,
                        grain=grain,
                        metric_definition=metric_definition,
                        dimension_name=dimension_name,
                    )
                    logger.info("Submitted task for pattern %s and dimension %s", pattern_name, dimension_name)
                    pattern_futures.append(dimension_future)

        # Wait for all pattern analyses to complete
        pattern_results = []
        for future in pattern_futures:
            result: dict[str, Any] = future.result()  # type: ignore
            pattern_results.append(result)

        # Create an artifact with the results
        await create_pattern_artifact(pattern_results=pattern_results, metric_id=metric_id, grain=grain)  # type: ignore

        # Prepare the analysis summary
        successful_patterns = [r for r in pattern_results if r["success"]]
        failed_patterns = [r for r in pattern_results if not r["success"]]

        # Extract pattern names
        pattern_names = list({r.get("pattern") for r in pattern_results})

        summary = {
            "tenant_id": tenant_id,
            "metric_id": metric_id,
            "grain": grain,
            "analysis_date": date.today().isoformat(),
            "patterns_executed": len(pattern_results),
            "patterns_successful": len(successful_patterns),
            "patterns_failed": len(failed_patterns),
            "pattern_names": pattern_names,
            "status": "success" if not failed_patterns else "partial_success" if successful_patterns else "failed",
            "error_patterns": failed_patterns,
        }

        # Emit the success/failure event
        event = "metric.pattern.analysis.success" if not failed_patterns else "metric.pattern.analysis.failed"
        emit_event(
            event=event,
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": tenant_id_str,
                "grain": grain,
            },
            payload=summary,
        )

        logger.info("Completed pattern analysis for metric %s, event: %s", metric_id, event)
    except Exception as e:
        logger.error("Error running pattern analysis for metric %s: %s", metric_id, str(e), exc_info=True)

        # Emit failure event
        emit_event(
            event="metric.pattern.analysis.failed",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": tenant_id_str,
                "grain": grain,
            },
            payload={
                "tenant_id": tenant_id,
                "metric_id": metric_id,
                "grain": grain,
                "error": {
                    "message": str(e),
                    "type": type(e).__name__,
                },
                "status": "failed",
                "timestamp": datetime.now().isoformat(),
            },
        )

        # Create error artifact
        await create_markdown_artifact(  # type: ignore
            key=f"pattern-runs-error-{metric_id.replace('_', '-').lower()}-{date.today().isoformat()}",
            markdown=f"# Pattern Analysis Error\n\n"
            f"- **Tenant ID**: {tenant_id}\n"
            f"- **Metric ID**: {metric_id}\n"
            f"- **Grain**: {grain}\n"
            f"- **Error Type**: {type(e).__name__}\n"
            f"- **Error Message**: {str(e)}\n",
        )

        raise
    finally:
        # Reset tenant context
        reset_context()


@flow(
    name="run-pattern",
    flow_run_name="run-pattern:tenant={tenant_id}_pattern={pattern_name}_metric={metric_id}_grain={grain}",
    description="Run a pattern for a specific metric and dimension (optional)",
)
async def trigger_run_pattern(
    tenant_id: int, pattern_name: str, metric_id: str, grain: Granularity, dimension_name: str | None = None
):
    """
    Trigger the run_pattern flow for a specific pattern, metric, and grain.

    Args:
        tenant_id: Tenant ID as integer
        pattern_name: Name of the pattern to run
        metric_id: ID of the metric to analyze
        grain: Granularity (day, week, month)
        dimension_name: Name of the dimension to analyze
    """
    logger = get_run_logger()

    # Set tenant context
    set_tenant_id(tenant_id)

    # Get the metric definition with dimensions
    metric_definition = await get_metric(metric_id=metric_id)

    logger.info(
        "Running pattern %s for metric %s, grain %s, dimension %s", pattern_name, metric_id, grain, dimension_name
    )
    # Run the pattern
    result = await run_pattern(
        pattern=pattern_name,
        metric_id=metric_id,
        grain=grain,
        metric_definition=metric_definition,
        dimension_name=dimension_name,
    )
    logger.info(
        "Completed run_pattern for pattern %s, metric %s, grain %s, dimension %s",
        pattern_name,
        metric_id,
        grain,
        dimension_name,
    )

    # Reset tenant context
    reset_context()

    return result
