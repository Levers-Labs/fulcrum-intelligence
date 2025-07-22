"""
Prefect artifacts creation utilities.
"""

from prefect.artifacts import create_markdown_artifact

from tasks_manager.tasks.semantic_manager import SyncSummary


async def create_tenant_cache_summary_artifact(summary: SyncSummary) -> None:
    """Create a markdown artifact with tenant cache summary."""
    status_emoji = {
        "success": "🟢",
        "partial_success": "🟡",
        "failed": "🔴",
        "skipped": "⚪",
    }

    emoji = status_emoji.get(summary["status"], "⚪")

    # Calculate totals from time series stats
    total_processed = summary["time_series_stats"]["processed"]
    total_failed = summary["time_series_stats"]["failed"]
    total_skipped = summary["time_series_stats"]["skipped"]
    total_metrics = summary["time_series_stats"]["total"]

    # Create markdown content
    markdown_content = f"""
# {emoji} Snowflake Cache Summary

**Tenant ID:** {summary['tenant_id']}
**Status:** {summary['status']}
**Sync Type:** {summary['sync_type']}
**Grain:** {summary['grain']}
**Period:** {summary['start_date']} to {summary['end_date']}

## Overview
- **Total Metrics:** {total_metrics}
- **Records Processed:** {total_processed}
- **Metrics Failed:** {total_failed}
- **Metrics Skipped:** {total_skipped}

## Tenant Cache Statistics
- **Metrics Processed:** {total_metrics}
- **Records Cached:** {total_processed}
- **Failed Metrics:** {total_failed}

*Note: This sync only includes metric-level time series data (no dimensional data)*
"""

    # Add failed tasks if any
    if summary["failed_tasks"]:
        markdown_content += "\n## Failed Tasks\n"
        for task in summary["failed_tasks"]:
            markdown_content += f"- **{task['task']}**: {task['error']}\n"

    # Add error if any
    if summary["error"]:
        markdown_content += f"\n## Error\n```\n{summary['error']}\n```\n"

    # Create artifact
    await create_markdown_artifact(  # type: ignore
        key=f"tenant-{summary['tenant_id']}-{summary['grain'].lower()}-cache-summary",
        markdown=markdown_content,
        description=f"Tenant cache summary for tenant {summary['tenant_id']} ({summary['grain']})",
    )
