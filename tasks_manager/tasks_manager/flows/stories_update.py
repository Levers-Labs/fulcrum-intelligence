from datetime import date

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context

from commons.models.enums import Granularity
from tasks_manager.tasks.stories_update import update_story_dates_for_tenant


@flow(
    name="update-story-dates",
    description="Update story dates for a specific tenant based on granularity",
)
async def update_story_dates(tenant_id: str, update_date: date = None):
    """
    Flow to update story dates for a specific tenant

    This flow increments story dates by one unit of their granularity:
    - Day grain: +1 day (updated daily)
    - Week grain: +1 week (updated only on Mondays)
    - Month grain: +1 month (updated only on 1st of month)
    - Quarter grain: +3 months (updated only on 1st day of quarter)
    - Year grain: +1 year (updated only on Jan 1st)

    Args:
        tenant_id: The tenant ID to run updates for.
        update_date: The date to use for determining if updates should run.
                    Defaults to today if not provided.
    """
    update_date = update_date or date.today()
    logger = get_run_logger()

    # Convert tenant_id to int
    tenant_id_int = int(tenant_id)

    logger.info(f"Starting story date updates for tenant {tenant_id_int} as of {update_date}")

    try:
        # Process the specified tenant
        result = await update_story_dates_for_tenant(tenant_id=tenant_id_int, current_date=update_date)

        # Get run context for additional information
        context = get_run_context()
        run_info = {}
        if hasattr(context, "flow_run") and context.flow_run:
            run_info = {
                "flow_run_id": context.flow_run.id,
                "flow_run_name": context.flow_run.name,
                "deployment_id": context.flow_run.deployment_id,
                "start_time": (
                    context.flow_run.start_time.strftime("%Y-%m-%dT%H:%M:%S")
                    if context.flow_run.start_time
                    else "Not available"
                ),
            }

        # Format all the grain updates as a string
        grain_updates = "\n".join(
            [f"- {grain}: {result['updates_by_grain'][grain]} stories updated" for grain in Granularity]
        )

        # Create markdown summary
        summary = f"""
        # Story Date Update Summary
        - Tenant ID: {tenant_id_int}
        - Update Date: {update_date}
        - Total Stories Updated: {result["stories_updated"]}
        - Status: {"COMPLETED" if result else "FAILED"}

        ## Updates by Granularity
        {grain_updates}

        ## Details
        - Day grain stories (+1 day): {result["updates_by_grain"][Granularity.DAY]} updated
        - Week grain stories (+7 days): {result["updates_by_grain"][Granularity.WEEK]} updated
        - Month grain stories (+1 month): {result["updates_by_grain"][Granularity.MONTH]} updated
        - Quarter grain stories (+3 months): {result["updates_by_grain"][Granularity.QUARTER]} updated
        - Year grain stories (+1 year): {result["updates_by_grain"][Granularity.YEAR]} updated

        ## Run Info
        - Flow Run ID: {run_info.get("flow_run_id", "Not available")}
        - Flow Run Name: {run_info.get("flow_run_name", "Not available")}
        - Deployment ID: {run_info.get("deployment_id", "Not available")}
        - Start Time: {run_info.get("start_time", "Not available")}
        """

        # Create markdown artifact
        await create_markdown_artifact(
            key=f"story-date-update-tenant-{tenant_id_int}-{update_date}",
            markdown=summary,
            description=f"Story date updates for tenant {tenant_id_int} on {update_date}",
        )

        logger.info(
            f"Completed story date updates for tenant {tenant_id_int}: {result['stories_updated']} stories " f"updated"
        )
        return result

    except Exception as e:
        logger.error(f"Error updating story dates for tenant {tenant_id_int}: {e}")
        raise
