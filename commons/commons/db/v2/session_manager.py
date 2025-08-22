from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.v2.engine import get_async_engine
from commons.utilities.context import get_tenant_id


def _build_session_factory(database_url: str, options: dict[str, Any]):
    engine = get_async_engine(database_url, options)
    return async_sessionmaker(
        bind=engine, class_=AsyncSession, autoflush=False, autocommit=False, expire_on_commit=False
    )


class AsyncSessionManager:
    def __init__(self, database_url: str, engine_options: dict[str, Any], max_concurrent_sessions: int | None = None):
        self._factory = _build_session_factory(database_url, engine_options)
        self._sem = asyncio.Semaphore(max_concurrent_sessions) if max_concurrent_sessions else None
        self._active = 0
        self._lock = asyncio.Lock()

    @property
    def current_session_count(self) -> int:
        return self._active

    @asynccontextmanager
    async def session(
        self,
        *,
        commit: bool = True,
        tenant_id: int | None = None,
        local_settings: dict[str, str] | None = None,
    ) -> AsyncGenerator[AsyncSession, None]:
        class _Noop:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: Any) -> bool:
                return False

        cm: Any = self._sem if self._sem else _Noop()

        async with cm:  # type: ignore
            async with self._lock:
                self._active += 1
            s = self._factory()
            try:
                tenant_id = tenant_id or get_tenant_id()
                if tenant_id is not None:
                    await s.execute(text("SET SESSION ROLE tenant_user;"))
                    await s.execute(text(f"SET app.current_tenant={tenant_id};"))
                if local_settings:
                    for k, v in local_settings.items():
                        await s.execute(text(f"SET LOCAL {k} = '{v}'"))
                yield s
                if commit:
                    await s.commit()
            except Exception:
                await s.rollback()
                raise
            finally:
                await s.close()
                async with self._lock:
                    self._active -= 1

    @asynccontextmanager
    async def batch_session(
        self,
        *,
        tenant_id: int | None = None,
        statement_timeout_ms: int = 900_000,
        work_mem: str | None = None,
        commit: bool = True,
    ) -> AsyncGenerator[AsyncSession, None]:
        _locals: dict[str, str] = {"statement_timeout": str(statement_timeout_ms)}
        if work_mem:
            _locals["work_mem"] = work_mem
        async with self.session(commit=commit, tenant_id=tenant_id, local_settings=_locals) as s:
            yield s
