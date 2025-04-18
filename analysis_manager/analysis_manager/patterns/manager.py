"""Manager for all pattern-related operations."""

import logging
from datetime import date
from typing import Any, TypeVar

from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.patterns.crud import PatternConfigCRUD, PatternCRUD
from analysis_manager.patterns.models.config import PatternConfig
from analysis_manager.patterns.models.pattern_result import PatternResult
from levers import Levers
from levers.models import BasePattern, PatternConfig as PatternConfigModel

logger = logging.getLogger(__name__)

PatternResultType = TypeVar("PatternResultType", bound=BasePattern)


class PatternManager:
    """Manager for pattern-related operations."""

    def __init__(self, session: AsyncSession):
        """Initialize the pattern manager.

        Args:
            session: Database session
        """
        self.session = session
        self.pattern_crud: PatternCRUD = PatternCRUD(PatternResult, session)
        self.config_crud: PatternConfigCRUD = PatternConfigCRUD(PatternConfig, session)
        self.levers: Levers = Levers()

    async def store_pattern_result(self, pattern_name: str, pattern_result: Any) -> PatternResult:
        """
        Store a pattern result.

        Args:
            pattern_name: The name of the pattern
            pattern_result: The pattern result from levers

        Returns:
            The created pattern result database model
        """
        return await self.pattern_crud.store_pattern_result(pattern_name, pattern_result)

    async def get_latest_pattern_result(self, pattern_name: str, metric_id: str) -> PatternResultType | None:
        """
        Get the latest pattern result for a metric.

        Args:
            pattern_name: The name of the pattern
            metric_id: The metric ID

        Returns:
            The latest pattern result as a Pydantic model or None if not found
        """
        db_result = await self.pattern_crud.get_latest_for_metric(pattern_name, metric_id)

        if not db_result:
            return None

        return self.pattern_crud.to_pattern_model(db_result)

    async def get_pattern_results(self, pattern_name: str, metric_id: str, limit: int = 10) -> list[PatternResultType]:
        """
        Get multiple pattern results for a metric.

        Args:
            pattern_name: The name of the pattern
            metric_id: The metric ID
            limit: Maximum number of results to return

        Returns:
            List of pattern results as Pydantic models
        """
        db_results = await self.pattern_crud.get_results_for_metric(pattern_name, metric_id, limit)

        if not db_results:
            return []

        results = []
        for db_result in db_results:
            model = self.pattern_crud.to_pattern_model(db_result)
            results.append(model)

        return results

    async def clear_pattern_results(
        self,
        metric_id: str,
        pattern_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """
        Clear pattern results for a metric.

        Args:
            metric_id: The metric ID
            pattern_name: Optional pattern name to filter by
            start_date: Optional start date of the range to clear
            end_date: Optional end date of the range to clear

        Returns:
            Operation results
        """
        return await self.pattern_crud.clear_data(
            metric_id=metric_id, pattern_name=pattern_name, start_date=start_date, end_date=end_date
        )

    # Configuration methods
    async def get_pattern_config(self, pattern_name: str) -> PatternConfigModel:
        """
        Get a pattern configuration.

        Args:
            pattern_name: The name of the pattern

        Returns:
            The pattern configuration
        """
        db_config = await self.config_crud.get_config(pattern_name)

        if db_config:
            return db_config.to_pydantic()

        return self.levers.get_pattern_default_config(pattern_name)

    async def list_pattern_configs(self) -> list[PatternConfigModel]:
        """
        List all pattern configurations.

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
