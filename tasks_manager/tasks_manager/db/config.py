from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.session import get_async_session as _get_async_session
from tasks_manager.config import AppConfig


# async session
async def get_async_session_gen() -> AsyncGenerator[AsyncSession, None]:
    """
    Get an async session generator
    """
    config = await AppConfig.load("default")
    async for session in _get_async_session(config.DATABASE_URL, config.SQLALCHEMY_ENGINE_OPTIONS):
        yield session


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get an async session
    """
    session = None
    try:
        async for _db_session in get_async_session_gen():
            session = _db_session
            yield session
            if session:
                await session.commit()
    except Exception:
        if session:
            await session.rollback()
        raise
    finally:
        if session:
            await session.close()
