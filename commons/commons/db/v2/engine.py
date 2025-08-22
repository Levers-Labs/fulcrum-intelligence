from __future__ import annotations

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from commons.db.engine import resolve_pool_class

_engines: dict[str, AsyncEngine] = {}


def _key(url: str, opts: dict[str, Any]) -> str:
    return f"{url}|{json.dumps(opts, sort_keys=True, default=str)}"


def get_async_engine(database_url: str, options: dict[str, Any] | None = None) -> AsyncEngine:
    opts = resolve_pool_class(options or {})
    k = _key(database_url, opts)
    eng = _engines.get(k)
    if eng is None:
        eng = create_async_engine(database_url, **opts)
        _engines[k] = eng
    return eng


async def dispose_async_engine(database_url: str, options: dict[str, Any] | None = None) -> None:
    opts = resolve_pool_class(options or {})
    k = _key(database_url, opts)
    if eng := _engines.pop(k, None):
        await eng.dispose()


async def dispose_all_async_engines() -> None:
    for eng in _engines.values():
        await eng.dispose()
    _engines.clear()
