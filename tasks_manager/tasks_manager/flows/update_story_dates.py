from datetime import date
from typing import List, Optional

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact

from commons.models.enums import Granularity
from tasks_manager.tasks.common import fetch_tenants


@flow(
    name="update-story-dates",
    description="Update story dates for all tenants and grains",
)
async def update_story_dates(
    tenant_ids: list[int] | None = None, current_date: date | None = None, grains: list[Granularity] | None = None
):
    """
    Main flow to update story dates for specified tenants and grains

    Args:
        tenant_ids: List of tenant IDs to update. If None, all tenants with story generation enabled are updated.
        current_date: The date to use as the current date. Defaults to today.
        grains: List of grains to update. If None, determines grains based on current date.
    """
    logger = get_run_logger()
    current_date = current_date or date.today()

    # Determine which grains to update based on the current date
    if grains is None:
        grains = []
        # Always update day grain
        grains.append(Granularity.DAY)

        # Update week grain on Mondays
        if current_date.weekday() == 0:  # Monday is 0
            grains.append(Granularity.WEEK)

        # Update month grain on the 1st of the month
        if current_date.day == 1:
            grains.append(Granularity.MONTH)

    # Get tenants
    if tenant_ids is None:
        tenants = await fetch_tenants(enable_story_generation=True)
        tenant_ids = [tenant["id"] for tenant in tenants]

    logger.info(
        f"Updating story dates for tenants {tenant_ids}, grains {[g.value for g in grains]}, current date {current_date}"
    )

    # Update story dates for each tenant and grain
    all_updated_stories = []
    for tenant_id in tenant_ids:
        for grain in grains:
            updated_stories = await update_story_dates_for_grain(
                tenant_id=tenant_id, grain=grain, current_date=current_date
            )
            all_updated_stories.extend(updated_stories)

    # Add updated stories as table artifact
    await create_table_artifact(
        key=f"{current_date}-updated-story-dates",
        table=all_updated_stories,
        description=f"Story dates updated for {current_date}",
    )

    logger.info(f"Updated {len(all_updated_stories)} story dates")

    return all_updated_stories
