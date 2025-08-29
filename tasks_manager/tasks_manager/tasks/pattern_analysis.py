import os
from datetime import date, datetime
from typing import Any

from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from analysis_manager.db.config import get_async_session as analysis_manager_session
from analysis_manager.patterns.manager import PatternManager
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from levers import Levers
from levers.models.common import AnalysisWindow
from levers.models.pattern_config import PatternConfig
from query_manager.core.schemas import MetricDetail
from query_manager.db.config import get_async_session as query_manager_session
from query_manager.semantic_manager.crud import SemanticManager
from tasks_manager.config import AppConfig
from tasks_manager.services.pattern_data_organiser import DataDict, PatternDataOrganiser


@task(
    name="fetch_metric_time_series",
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=300,  # 5 minutes
    task_run_name="fetch-metric-time-series-{metric_id}-{grain}",
)
async def fetch_pattern_data(
    pattern_config: PatternConfig,
    metric_id: str,
    grain: Granularity,
    metric_definition: MetricDetail,
    dimension_name: str | None = None,
) -> DataDict:
    """
    Fetch pattern data for analysis.

    Args:
        pattern_config: Pattern configuration
        metric_id: Metric ID
        grain: Data granularity
        metric_definition: Metric definition
        dimension_name: Optional dimension name for dimension-based patterns

    Returns:
        DataDict with data for the pattern for all the pattern data sources
    """
    logger = get_run_logger()

    try:
        # Set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host

        async with query_manager_session() as session:
            semantic_manager = SemanticManager(session)
            # create a pattern data organiser
            data_organiser = PatternDataOrganiser(semantic_manager=semantic_manager)
            # fetch data for the pattern
            fetch_kwargs = {}
            # Add dimension name if it exists
            if dimension_name:
                fetch_kwargs["dimension_name"] = dimension_name
            data = await data_organiser.fetch_data_for_pattern(
                config=pattern_config,
                metric_id=metric_id,
                grain=grain,  # type: ignore
                metric_definition=metric_definition,
                **fetch_kwargs,
            )
            logger.info("retrieved data for pattern %s", pattern_config.pattern_name)
            return data
    except Exception as e:
        logger.error("Error fetching metric data for metric %s, grain %s: %s", metric_id, grain, str(e))
        raise e


def create_analysis_window(pattern_config: PatternConfig, grain: Granularity) -> AnalysisWindow:
    """
    Create an analysis window based on pattern config and grain.

    Args:
        pattern_config: Pattern configuration
        grain: Data granularity

    Returns:
        AnalysisWindow object
    """
    # Get the date range for the analysis window
    start_date, end_date = pattern_config.analysis_window.get_date_range(grain=grain)  # type: ignore

    return AnalysisWindow(start_date=start_date.isoformat(), end_date=end_date.isoformat(), grain=grain)  # type: ignore


@task(
    name="get_pattern_config",
    retries=1,
    retry_delay_seconds=15,
    timeout_seconds=120,  # 2 minutes
    task_run_name="get-pattern-config-{pattern}",
)
async def get_pattern_config(pattern: str) -> PatternConfig:
    """
    Get the pattern configuration for a pattern.
    """
    logger = get_run_logger()

    try:
        # Set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.analysis_manager_server_host

        # Get the saved pattern configuration or the default one
        async with analysis_manager_session() as session:
            pattern_manager = PatternManager(session)
            pattern_config = await pattern_manager.get_pattern_config(pattern)
            return pattern_config
    except Exception as e:
        logger.warning("Error getting pattern config: %s. Using default config.", str(e))
        raise


@task(
    name="run_pattern",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=600,  # 10 minutes
    task_run_name="run-pattern-{pattern}-{metric_id}-{grain}-{dimension_name}",
)
async def run_pattern(
    pattern: str,
    metric_id: str,
    grain: Granularity,
    metric_definition: MetricDetail,
    dimension_name: str | None = None,
) -> dict[str, Any]:
    """
    Run a pattern on metric data.

    Args:
        pattern: name of the pattern to run
        metric_id: Metric ID
        grain: Data granularity
        metric_definition: Metric definition
        dimension_name: Optional dimension name for dimension-based patterns

    Returns:
        Dictionary containing pattern analysis result or error information
    """
    logger = get_run_logger()
    tenant_id = get_tenant_id()
    levers: Levers = Levers()

    dimension_suffix = f" dimension- {dimension_name}" if dimension_name else ""
    # Emit "pattern analysis started" event
    emit_event(
        event="pattern.run.started",
        resource={
            "prefect.resource.id": f"metric.{metric_id}",
            "metric_id": metric_id,
            "tenant_id": str(tenant_id),
            "grain": grain.value,
            "pattern": pattern,
            **({"dimension": dimension_name} if dimension_name else {}),
        },
        payload={
            "pattern": pattern,
            "metric_id": metric_id,
            "tenant_id": tenant_id,
            "grain": grain.value,
            "timestamp": datetime.now().isoformat(),
            **({"dimension": dimension_name} if dimension_name else {}),
        },
    )

    try:
        # Get the pattern config
        pattern_config = await get_pattern_config(pattern)  # type: ignore

        # Fetch pattern data
        data = await fetch_pattern_data(
            pattern_config=pattern_config,
            metric_id=metric_id,
            grain=grain,
            metric_definition=metric_definition,
            dimension_name=dimension_name,
        )
        logger.info("Fetched data for pattern %s", pattern)

        # Check if data is available for all data sources
        # If not, emit a skip event
        for key, df in data.items():
            if df.empty:
                logger.warning("Empty dataset for %s in pattern %s", key, pattern)
                # Emit the skip event
                emit_event(
                    event="metric.pattern.analysis.skipped",
                    resource={
                        "prefect.resource.id": f"metric.{metric_id}",
                        "metric_id": metric_id,
                        "grain": grain,
                        "pattern": pattern,
                        **({"dimension": dimension_name} if dimension_name else {}),
                    },
                    payload={
                        "status": "skipped",
                        "reason": "no_data",
                        "data_source": key,
                        "pattern": pattern,
                        "grain": grain,
                        "metric_id": metric_id,
                        "tenant_id": tenant_id,
                        **({"dimension": dimension_name} if dimension_name else {}),
                    },
                )
                return {
                    "success": False,
                    "pattern": pattern,
                    "metric_id": metric_id,
                    "grain": grain,
                    "key": key,
                    "error": {"message": "No data available", "type": "NoDataAvailableError"},
                    **({"dimension_name": dimension_name} if dimension_name else {}),
                }

        # Create an analysis window based on data
        analysis_window = create_analysis_window(pattern_config=pattern_config, grain=grain)

        # prepare the data arguments for the pattern run
        data_args: dict[str, Any] = {"metric_id": metric_id}
        if dimension_name:
            data_args["dimension_name"] = dimension_name
        for key, _key_data in data.items():
            data_args[key] = _key_data

        logger.info("Running pattern %s for metric %s%s", pattern, metric_id, dimension_suffix)

        # Run pattern analysis
        result = levers.execute_pattern(
            pattern_name=pattern, analysis_window=analysis_window, config=pattern_config, **data_args
        )
        logger.info("Successfully ran pattern %s for metric %s%s", pattern, metric_id, dimension_suffix)

        # Store the pattern result
        stored_result = await store_pattern_result(result=result)

        # Emit "pattern analysis success" event with result data
        emit_event(
            event="pattern.run.success",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": str(tenant_id),
                "grain": grain.value,
                "pattern": pattern,
                **({"dimension": dimension_name} if dimension_name else {}),
            },
            payload=stored_result,
        )

        # Return result as dictionary with success flag
        # Check if there's an error object in the stored result
        success = False if stored_result.get("error") else True
        return {"success": success, **stored_result}
    except Exception as e:
        logger.error("Error running pattern %s for metric %s%s: %s", pattern, metric_id, dimension_suffix, str(e))

        failed_result = {
            "success": False,
            "pattern": pattern,
            "metric_id": metric_id,
            "timestamp": datetime.now().isoformat(),
            "error": {"message": str(e), "type": type(e).__name__},
            **({"dimension_name": dimension_name} if dimension_name else {}),
        }

        # Emit "pattern analysis failed" event
        emit_event(
            event="pattern.run.failed",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": str(tenant_id),
                "grain": grain.value,
                "pattern": pattern,
                **({"dimension": dimension_name} if dimension_name else {}),
            },
            payload=failed_result,
        )

        # Return error result as dictionary
        return failed_result


@task(name="store_pattern_result", retries=2, task_run_name="store-pattern-result")
async def store_pattern_result(result: Any) -> dict[str, Any]:
    """
    Store pattern analysis result in the analysis store.

    Args:
        result: Pattern analysis result

    Returns:
        Stored pattern result with metadata
    """
    logger = get_run_logger()

    pattern_name = result.pattern
    metric_id = result.metric_id

    logger.info("Storing pattern result for metric %s, pattern %s", metric_id, pattern_name)

    # Set the server host for the query manager
    config = await AppConfig.load("default")
    os.environ["SERVER_HOST"] = config.analysis_manager_server_host

    async with analysis_manager_session() as session:
        pattern_manager = PatternManager(session)

        # Store result using an analysis manager client
        stored_result = await pattern_manager.store_pattern_result(pattern_name=pattern_name, pattern_result=result)

        logger.info("Successfully stored pattern result for metric %s, pattern %s", metric_id, pattern_name)
        return stored_result.model_dump(mode="json")


@task(name="create_pattern_artifact", retries=1, task_run_name="create-pattern-runs-artifact-{metric_id}-{grain}")
async def create_pattern_artifact(pattern_results: list[dict[str, Any]], metric_id: str, grain: Granularity):
    """
    Create an artifact with pattern analysis results.

    Args:
        pattern_results: List of pattern analysis results (dictionaries)
        metric_id: Metric ID
        grain: Granularity
    """
    tenant_id = get_tenant_id()
    logger = get_run_logger()
    logger.info("Creating artifact for tenant %s, metric %s", tenant_id, metric_id)

    # Separate metric-based and dimension-based patterns
    metric_patterns = [r for r in pattern_results if not r.get("dimension_name")]
    dimension_patterns = [r for r in pattern_results if r.get("dimension_name")]

    # Summary section
    summary = f"# Pattern Runs for Metric {metric_id}\n\n"
    summary += f"- **Analysis Date**: {date.today().isoformat()}\n"
    summary += f"- **Granularity**: {grain.value}\n"
    summary += f"- **Metric ID**: {metric_id}\n"
    summary += f"- **Tenant ID**: {tenant_id}\n"
    summary += f"- **Total Patterns Executed**: {len(pattern_results)}\n"
    summary += f"- **Metric-based Patterns**: {len(metric_patterns)}\n"
    summary += f"- **Dimension-based Patterns**: {len(dimension_patterns)}\n\n"

    # Metric-based patterns table
    if metric_patterns:
        summary += "## Metric-based Pattern Runs\n\n"
        summary += "| Result ID | Pattern | Version | Status | Error Type | Error Message |\n"
        summary += "|-----------|---------|---------|--------|------------|---------------|\n"

        for result in metric_patterns:
            pattern_name = result["pattern"]
            success = result["success"]
            version = result.get("version", "N/A")
            result_id = result.get("id", "N/A")
            status = "✅ Success" if success else "❌ Failed"

            if not success:
                error = result["error"]
                error_message = error.get("message", "Unknown error")
                error_type = error.get("type", "Unknown")
            else:
                error_message = ""
                error_type = ""

            # Add row to the table
            summary += f"| {result_id} | {pattern_name} | {version} | {status} | {error_type} | {error_message} |\n"

        summary += "\n"

    # Dimension-based patterns table
    if dimension_patterns:
        summary += "## Dimension-based Pattern Runs\n\n"
        summary += "| Result ID | Pattern | Dimension | Version | Status | Error Type | Error Message |\n"
        summary += "|-----------|---------|-----------|---------|--------|------------|---------------|\n"

        for result in dimension_patterns:
            pattern_name = result["pattern"]
            success = result["success"]
            version = result.get("version", "N/A")
            result_id = result.get("id", "N/A")
            dimension_name = result.get("dimension_name", "N/A")
            status = "✅ Success" if success else "❌ Failed"

            if not success:
                error = result["error"]
                error_message = error.get("message", "Unknown error")
                error_type = error.get("type", "Unknown")
            else:
                error_message = ""
                error_type = ""

            # Add row to the table
            summary += f"| {result_id} | {pattern_name} | {dimension_name} | {version} | {status} | {error_type} | {error_message} |\n"  # noqa

        summary += "\n"

    # Create the artifact
    await create_markdown_artifact(  # type: ignore
        key=f"pattern-analysis-{metric_id.replace('_', '-').lower()}-{grain.value.lower()}-{date.today().isoformat()}",
        markdown=summary,  # noqa
    )

    logger.info("Successfully created artifact for metric %s", metric_id)
