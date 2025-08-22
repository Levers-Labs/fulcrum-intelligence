from __future__ import annotations

import json
from typing import Any, AsyncContextManager

from dagster import ConfigurableResource
from sqlmodel.ext.asyncio.session import AsyncSession

from asset_manager.resources.config import AppConfigResource
from commons.db.v2 import AsyncSessionManager, build_engine_options


class DbResource(ConfigurableResource):
    """
    Dagster resource providing database session manager.

    Usage:
        db = DbResource(app_config=app_config)
        async with db.session() as session:
            # use session
    """

    app_config: AppConfigResource

    profile: str = "prod"
    engine_overrides_json: str | None = None
    max_concurrent_sessions: int = 80

    _manager: AsyncSessionManager | None = None

    def _engine_options(self) -> dict[str, Any]:
        overrides = json.loads(self.engine_overrides_json) if self.engine_overrides_json else None
        return build_engine_options(self.profile, overrides, app_name="asset_manager")

    def _get_manager(self) -> AsyncSessionManager:
        if self._manager is None:
            s = self.app_config.settings
            self._manager = AsyncSessionManager(
                database_url=s.database_url,
                engine_options=self._engine_options(),
                max_concurrent_sessions=self.max_concurrent_sessions,
            )
        return self._manager

    def session(self, *, tenant_id: int | None = None, commit: bool = True) -> AsyncContextManager[AsyncSession]:
        return self._get_manager().session(tenant_id=tenant_id, commit=commit)

    def batch_session(
        self,
        *,
        tenant_id: int | None = None,
        statement_timeout_ms: int = 900_000,
        work_mem: str | None = None,
        commit: bool = True,
    ) -> AsyncContextManager[AsyncSession]:
        return self._get_manager().batch_session(
            tenant_id=tenant_id,
            statement_timeout_ms=statement_timeout_ms,
            work_mem=work_mem,
            commit=commit,
        )
