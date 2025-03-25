from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger, unmapped
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.tasks.pattern_analysis import create_pattern_artifact, fetch_metric_time_series, run_pattern


@flow(
    name="pattern-analysis-metric",
    flow_run_name="pattern-analysis-metric:tenant={tenant_id_str}_metric={metric_id}_grain={grain}",
    description="Run pattern analysis for a specific metric",
    retries=2,
    retry_delay_seconds=15,
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
        # TODO: Uncomment this when we have generic analyser to figure out what data patterns need to be ran
        # Get all patterns to run using Levers API
        # levers = Levers()

        # available_patterns = levers_api.list_patterns()
        # logger.info("Found %s available patterns: %s", len(available_patterns), available_patterns)

        # if not available_patterns:
        #     logger.warning("No patterns available for analysis")
        #     return {
        #         "status": "skipped",
        #         "reason": "no_patterns",
        #         "tenant_id": tenant_id,
        #         "metric_id": metric_id,
        #         "grain": grain,
        #     }

        available_patterns = ["performance_status"]

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
        # metric data
        df = await fetch_metric_time_series(metric_id=metric_id, grain=grain)

        if df.empty:
            logger.warning("No data available for metric %s", metric_id)
            # Emit the skip event
            emit_event(
                event="metric.pattern.analysis.skipped",
                resource={
                    "prefect.resource.id": f"metric.{metric_id}",
                    "metric_id": metric_id,
                    "tenant_id": tenant_id_str,
                    "grain": grain,
                },
                payload={"status": "skipped", "reason": "no_data"},
            )

        # Run analysis for each pattern in parallel
        pattern_futures = run_pattern.map(
            pattern=available_patterns, metric_id=metric_id, data=unmapped(df), grain=grain
        )

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

        # Extract pattern names (now always available in the dictionary)
        pattern_names = [r["pattern"] for r in pattern_results]

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
            key=f"pattern-runs-error-{metric_id.lower()}-{date.today().isoformat()}",
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
