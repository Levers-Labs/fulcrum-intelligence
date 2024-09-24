import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from query_manager.config import get_settings
from story_manager.core.heuristics import STORY_TYPE_HEURISTIC_MAPPING
from story_manager.core.models import HeuristicExpression

# Set up logging
logger = logging.getLogger(__name__)


async def insert_data(session: AsyncSession) -> None:
    logger.info("Inserting Story Salience Heuristic Expressions")
    for story_type, grains in STORY_TYPE_HEURISTIC_MAPPING.items():
        for k, v in grains.items():
            stmt = (
                insert(HeuristicExpression)
                .values(story_type=story_type, grain=k, expression=v)
                .on_conflict_do_update(index_elements=["story_type", "grain"], set_=dict(expression=v))
            )
            await session.execute(stmt)
    await session.commit()
    logger.info("Salience Heuristic Expressions Insert Completed")


async def main() -> None:
    """
    Main function to run the upsert process.
    """
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with async_session() as session:
        await insert_data(session)


# Usage example:
if __name__ == "__main__":
    asyncio.run(main())
