"""Manager for all pattern-related operations."""

import logging
from typing import Any, TypeVar

from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.patterns.crud.base import PatternCRUD
from analysis_manager.patterns.crud.performance_status import PerformanceStatusCRUD

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Any)
P = TypeVar("P", bound=PatternCRUD)


class PatternManager:
    """Manager for pattern-related operations."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self._pattern_cruds: dict[str, PatternCRUD] = {"performance_status": PerformanceStatusCRUD(session)}

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
