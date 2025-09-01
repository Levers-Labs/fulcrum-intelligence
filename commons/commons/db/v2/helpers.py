from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from pydantic_settings import BaseSettings
from sqlmodel.ext.asyncio.session import AsyncSession

from .lifecycle import dispose_session_manager, get_session_manager, init_session_manager


@asynccontextmanager
async def async_session(settings: BaseSettings, *, app_name: str | None = None) -> AsyncGenerator[AsyncSession, None]:
    """
    Generic async-session context manager for scripts / notebooks / workers.

        from analysis_manager.config import get_settings
        async with async_session(get_settings(), app_name="analysis_loader") as session:
            ...

    Args:
        settings: Service settings object containing database configuration
        app_name: Optional identifier for the SessionManager-v2 pool

    • First call in a process creates the engine & SessionManager
    • Later calls re-use the same manager
    """
    init_session_manager(settings, app_name=app_name)
    try:
        async with get_session_manager().session() as session:
            yield session
    finally:
        await dispose_session_manager()
