import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from query_manager.config import get_settings
from story_manager.core.heuristics import STORY_TYPE_HEURISTIC_MAPPING
from story_manager.core.models import StoryConfig

# Set up logging
logger = logging.getLogger(__name__)


async def update_story_config(session: AsyncSession) -> None:
    logger.info("Updating Story Config with Salience Heuristic Expressions")
    for story_type, grains in STORY_TYPE_HEURISTIC_MAPPING.items():
        for grain, expr in grains.items():
            stmt = (
                insert(StoryConfig)
                .values(story_type=story_type, grain=grain, heuristic_expression=expr)
                .on_conflict_do_update(index_elements=["story_type", "grain"], set_=dict(heuristic_expression=expr))
            )
            await session.execute(stmt)
    await session.commit()
    logger.info("Story Config Update Completed")


async def main() -> None:
    """
    Main function to run the upsert process.
    """
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with async_session() as session:
        await update_story_config(session)


# Usage example:
if __name__ == "__main__":
    asyncio.run(main())
