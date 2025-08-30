from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.session import get_session as _get_session
from commons.db.v2 import (
    AsyncSessionManager,
    dispose_session_manager,
    get_session_manager,
    init_session_manager,
)
from story_manager.config import get_settings

# Used to load models for alembic migrations
MODEL_PATHS = ["story_manager.core.models"]


# sync session
def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    with _get_session(settings.DATABASE_URL, settings.SQLALCHEMY_ENGINE_OPTIONS) as session:  # type: ignore
        yield session


# async session manager
async def get_async_session_manager() -> AsyncSessionManager:
    return get_session_manager()


AsyncSessionManagerDep = Annotated[AsyncSessionManager, Depends(get_async_session_manager)]


# async session
async def get_async_session(mgr: AsyncSessionManagerDep) -> AsyncGenerator[AsyncSession, None]:
    """Get a Session v2 async session with isolated connection per request."""
    async with mgr.session(commit=False) as session:
        yield session


async def get_batch_session(mgr: AsyncSessionManagerDep) -> AsyncGenerator[AsyncSession, None]:
    """Get a Session v2 batch session for long-running queries with elevated timeouts."""
    async with mgr.batch_session(commit=False) as session:
        yield session


# Session Dependency
SessionDep = Annotated[Session, Depends(get_session)]
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session)]


@asynccontextmanager
async def open_async_session(app_name: str | None = None) -> AsyncGenerator[AsyncSession, None]:
    """
    Context-manager for scripts / notebooks / Prefect tasks.

        async with open_async_session("story_script") as session:
            ...

    Args:
        app_name: Optional identifier for the SessionManager-v2 pool.
                  • None  → use default (service name)
                  • str   → custom name (e.g. "story_loader_script", "tasks_manager")
    """
    init_session_manager(get_settings(), app_name=app_name)
    try:
        async with get_session_manager().session() as session:
            yield session
    finally:
        await dispose_session_manager()
