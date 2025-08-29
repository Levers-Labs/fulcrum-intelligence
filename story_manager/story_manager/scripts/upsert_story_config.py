import argparse
import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.mappings import STORY_TYPE_HEURISTIC_MAPPING
from story_manager.core.models import StoryConfig
from story_manager.core.v2.heuristics_mappings import STORY_TYPE_HEURISTIC_MAPPING_V2
from story_manager.scripts.db_utils import async_db_session

# Set up logging
logger = logging.getLogger(__name__)


async def upsert_story_config(session: AsyncSession, tenant_id: int, version: int) -> None:
    logger.info("Running Story Config Upsert with Salience Heuristic Expressions for Tenant ID: %s", tenant_id)
    if version == 1:
        story_config_mapping = STORY_TYPE_HEURISTIC_MAPPING
    elif version == 2:
        story_config_mapping = STORY_TYPE_HEURISTIC_MAPPING_V2
    for story_type, grains in story_config_mapping.items():
        for grain, data in grains.items():
            expr = data["salient_expression"]
            cool_off = data["cool_off_duration"]
            stmt = (
                insert(StoryConfig)
                .values(
                    story_type=story_type,
                    grain=grain,
                    heuristic_expression=expr,
                    cool_off_duration=cool_off,
                    tenant_id=tenant_id,
                    version=version,
                )
                .on_conflict_do_update(
                    index_elements=["story_type", "grain", "tenant_id", "version"],
                    set_=dict(heuristic_expression=expr, cool_off_duration=cool_off),
                )
            )
            await session.execute(stmt)
    await session.commit()
    logger.info("Story Config Upsert Completed for Tenant ID: %s, Version: %s", tenant_id, version)


@async_db_session()
async def main(session, tenant_id: int, version: int) -> None:
    """
    Main function to run the upsert process.
    """
    # Set tenant id context in the db session
    logger.info("Setting tenant context, Tenant ID: %s", tenant_id)
    set_tenant_id(tenant_id)

    try:
        await upsert_story_config(session, tenant_id, version)
    finally:
        # clear context
        reset_context()


# Usage example:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upsert Story Config for a specific tenant.")
    parser.add_argument("tenant_id", type=str, help="The tenant ID for which to upsert the story config.")
    parser.add_argument("version", type=int, help="The version of the story config to upsert.")
    args = parser.parse_args()

    asyncio.run(main(int(args.tenant_id), int(args.version)))
