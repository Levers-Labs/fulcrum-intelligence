import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from query_manager.config import get_settings
from story_manager.core.mappings import STORY_TYPE_HEURISTIC_MAPPING
from story_manager.core.models import StoryConfig

# Set up logging
logger = logging.getLogger(__name__)


async def upsert_story_config(session: AsyncSession) -> None:
    logger.info("Running Story Config Upsert with Salience Heuristic Expressions")
    for story_type, grains in STORY_TYPE_HEURISTIC_MAPPING.items():
        for grain, data in grains.items():
            expr = data["salient_expression"]
            cool_off = data["cool_off_duration"]
            stmt = (
                insert(StoryConfig)
                .values(story_type=story_type, grain=grain, heuristic_expression=expr, cool_off_duration=cool_off)
                .on_conflict_do_update(
                    index_elements=["story_type", "grain"],
                    set_=dict(heuristic_expression=expr, cool_off_duration=cool_off),
                )
            )
            await session.execute(stmt)
    await session.commit()
    logger.info("Story Config Upsert Completed")


async def main() -> None:
    """
    Main function to run the upsert process.
    """
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with async_session() as session:
        await upsert_story_config(session)


# Usage example:
if __name__ == "__main__":
    asyncio.run(main())
