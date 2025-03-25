import os
from datetime import date, datetime
from typing import Any

import pandas as pd
from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from analysis_manager.patterns.manager import PatternManager
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from levers.api import Levers
from levers.models.common import AnalysisWindow
from query_manager.db.config import get_async_session
from query_manager.semantic_manager.crud import SemanticManager
from tasks_manager.config import AppConfig


@task(
    name="fetch_metric_time_series",
    retries=2,
    retry_delay_seconds=15,
    task_run_name="fetch-metric-time-series-{metric_id}-{grain}",
)
async def fetch_metric_time_series(
    metric_id: str, grain: Granularity, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    """
    Fetch metric time series data for analysis.

    Args:
        metric_id: Metric ID
        grain: Data granularity
        start_date: Start date for data retrieval (optional)
        end_date: End date for data retrieval (optional)

    Returns:
        DataFrame with metric time series data that includes:
            - date
            - value
            - target
    """
    logger = get_run_logger()

    try:
        # Set the server host for the query manager
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host

        async with get_async_session() as session:
            semantic_manager = SemanticManager(session)

            # Fetch metric data directly from semantic manager
            data = await semantic_manager.get_metric_time_series_with_targets(
                metric_id=metric_id,
                grain=grain,
                start_date=start_date,
                end_date=end_date,
            )

            # Convert to pandas DataFrame
            df = pd.DataFrame(data)

            logger.info("retrieved %s rows of data: %s", len(df), df.head())
            return df
    except Exception as e:
        logger.error("Error fetching metric data for metric %s, grain %s: %s", metric_id, grain, str(e))
        raise e


@task(
    name="prepare_pattern_data",
    retries=1,
    retry_delay_seconds=15,
    task_run_name="prepare-pattern-data-{pattern}-{metric_id}-{grain}",
)
async def prepare_pattern_data(pattern: str, data: pd.DataFrame, metric_id: str, grain: Granularity) -> pd.DataFrame:
    """
    Prepare data specifically for a pattern's requirements.

    Args:
        pattern: name of the pattern to run
        data: Raw DataFrame with metric data
        metric_id: Metric ID
        grain: Data granularity

    Returns:
        Prepared DataFrame for the specific pattern
    """
    logger = get_run_logger()
    logger.info("Preparing data for pattern %s", pattern)

    # Add custom data preparation for the pattern
    logger.info("Data prepared for pattern %s", pattern)
    return data


def create_analysis_window(data: pd.DataFrame, grain: Granularity) -> AnalysisWindow:
    """
    Create an analysis window based on the data.

    Args:
        data: DataFrame with metric data
        grain: Data granularity

    Returns:
        AnalysisWindow object
    """
    # Use data range from DataFrame
    date_column = "date"
    if date_column in data.columns:
        start_date = data[date_column].min().isoformat()
        end_date = data[date_column].max().isoformat()
    else:
        raise ValueError("No date column found in the data")

    return AnalysisWindow(start_date=start_date, end_date=end_date, grain=grain)  # type: ignore


@task(
    name="run_pattern",
    retries=1,
    retry_delay_seconds=30,
    task_run_name="run-pattern-{pattern}-{metric_id}-{grain}",
)
async def run_pattern(pattern: str, metric_id: str, grain: Granularity, data: pd.DataFrame) -> dict[str, Any]:
    """
    Run a pattern on metric data.

    Args:
        pattern: name of the pattern to run
        metric_id: Metric ID
        grain: Data granularity
        data: DataFrame with metric data

    Returns:
        Dictionary containing pattern analysis result or error information
    """
    logger = get_run_logger()
    tenant_id = get_tenant_id()
    levers: Levers = Levers()

    # Emit "pattern analysis started" event
    emit_event(
        event="pattern.run.started",
        resource={
            "prefect.resource.id": f"metric.{metric_id}",
            "metric_id": metric_id,
            "tenant_id": str(tenant_id),
            "grain": grain.value,
            "pattern": pattern,
        },
        payload={
            "pattern": pattern,
            "metric_id": metric_id,
            "tenant_id": tenant_id,
            "grain": grain.value,
            "timestamp": datetime.now().isoformat(),
        },
    )

    try:
        # Check if pattern requires special data preparation
        data = await prepare_pattern_data(pattern, data=data, metric_id=metric_id, grain=grain)  # type: ignore

        # Create an analysis window based on data
        analysis_window = create_analysis_window(data=data, grain=grain)

        logger.info("Running pattern %s for metric %s", pattern, metric_id)
        # Run pattern analysis
        result = levers.execute_pattern(
            pattern_name=pattern, metric_id=metric_id, data=data, analysis_window=analysis_window
        )
        logger.info("Successfully ran pattern %s for metric %s", pattern, metric_id)

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
            },
            payload=stored_result,
        )

        # Return result as dictionary with success flag
        # Check if there's an error object in the stored result
        success = False if stored_result.get("error") else True
        return {"success": success, **stored_result}

    except Exception as e:
        logger.error("Error running pattern %s for metric %s: %s", pattern, metric_id, str(e))

        failed_result = {
            "success": False,
            "pattern": pattern,
            "metric_id": metric_id,
            "timestamp": datetime.now().isoformat(),
            "error": {"message": str(e), "type": type(e).__name__},
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
    os.environ["SERVER_HOST"] = config.query_manager_server_host

    async with get_async_session() as session:
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

    # Summary section
    summary = f"# Pattern Runs for Metric {metric_id}\n\n"
    summary += f"- **Analysis Date**: {date.today().isoformat()}\n"
    summary += f"- **Granularity**: {grain.value}\n"
    summary += f"- **Metric ID**: {metric_id}\n"
    summary += f"- **Tenant ID**: {tenant_id}\n"
    summary += f"- **Patterns Executed**: {len(pattern_results)}\n\n"

    # Results for each pattern
    # Create a table header for pattern results
    summary += "## Pattern Runs\n\n"
    summary += "| Result ID | Pattern | Version | Status | Error Type | Error Message |\n"
    summary += "|-----------|---------|---------|--------|------------|---------------|\n"

    for result in pattern_results:
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

    # Create the artifact
    await create_markdown_artifact(  # type: ignore
        key=f"pattern-analysis-{metric_id.lower()}-{grain.value.lower()}-{date.today().isoformat()}", markdown=summary
    )

    logger.info("Successfully created artifact for metric %s", metric_id)
