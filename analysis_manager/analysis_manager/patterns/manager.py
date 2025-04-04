"""Manager for all pattern-related operations."""

import logging
from typing import Any, TypeVar

from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.patterns.crud.base import PatternCRUD
from analysis_manager.patterns.crud.config import PatternConfigCRUD
from analysis_manager.patterns.crud.performance_status import PerformanceStatusCRUD
from analysis_manager.patterns.models.config import PatternConfig
from levers import Levers
from levers.models import PatternConfig as PatternConfigModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Any)
P = TypeVar("P", bound=PatternCRUD)


class PatternManager:
    """Manager for pattern-related operations."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self._pattern_cruds: dict[str, PatternCRUD] = {"performance_status": PerformanceStatusCRUD(session)}
        self.config_crud = PatternConfigCRUD(PatternConfig, session)

    def _get_pattern_crud(self, pattern_name: str) -> PatternCRUD:
        """
        Get the CRUD instance for a pattern.

        Args:
            pattern_name: The name of the pattern

        Returns:
            The pattern CRUD instance

        Raises:
            ValueError: If the pattern is not found
        """
        if pattern_name not in self._pattern_cruds:
            raise ValueError(f"Unknown pattern: {pattern_name}")
        return self._pattern_cruds[pattern_name]

    async def store_pattern_result(self, pattern_name: str, pattern_result: Any) -> Any:
        """
        Store a pattern result based on the pattern name.

        Args:
            pattern_name: The name of the pattern
            pattern_result: The pattern result from levers

        Returns:
            The created pattern result database model
        """
        crud = self._get_pattern_crud(pattern_name)
        return await crud.store_pattern_result(pattern_result)

    async def get_latest_pattern_result(self, pattern_name: str, metric_id: str) -> Any:
        """
        Get the latest pattern result for a metric.

        Args:
            pattern_name: The name of the pattern
            metric_id: The metric ID

        Returns:
            The latest pattern result or None
        """
        crud = self._get_pattern_crud(pattern_name)
        db_result = await crud.get_latest_for_metric(metric_id)
        if db_result:
            return await crud.to_pattern_result(db_result)
        return None

    # Configuration methods
    async def get_pattern_config(self, pattern_name: str) -> PatternConfigModel:
        """
        Get a pattern configuration.

        Args:
            pattern_name: The name of the pattern
        Returns:
            The pattern configuration or None
        """
        db_config = await self.config_crud.get_config(pattern_name)

        if db_config:
            return db_config.to_pydantic()

        levers: Levers = Levers()
        return levers.get_pattern_default_config(pattern_name)

    async def list_pattern_configs(self) -> list[PatternConfigModel]:
        """
        List all pattern configurations for a tenant.

        Returns:
            List of pattern configurations
        """
        db_configs = await self.config_crud.list_configs()
        return [config.to_pydantic() for config in db_configs]

    async def store_pattern_config(self, config: PatternConfigModel) -> PatternConfig:
        """
        Store a pattern configuration.

        Args:
            config: The pattern configuration

        Returns:
            The stored pattern configuration
        """
        return await self.config_crud.create_or_update_config(config)

    async def delete_pattern_config(self, pattern_name: str) -> bool:
        """
        Delete a pattern configuration.

        Args:
            pattern_name: The name of the pattern

        Returns:
            True if the configuration was deleted
        """
        return await self.config_crud.delete_config(pattern_name)
