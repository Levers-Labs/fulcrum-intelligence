"""CRUD operations for pattern configuration."""

import logging
from typing import cast

from sqlalchemy import select

from analysis_manager.patterns.models.config import PatternConfig
from commons.db.crud import CRUDBase
from commons.db.filters import BaseFilter
from levers.models import PatternConfig as PatternConfigModel

logger = logging.getLogger(__name__)


class PatternConfigCRUD(CRUDBase[PatternConfig, PatternConfigModel, PatternConfigModel, BaseFilter]):
    """CRUD operations for pattern configurations."""

    async def get_config(self, pattern_name: str) -> PatternConfig | None:
        """
        Get a pattern configuration.

        Args:
            pattern_name: The name of the pattern

        Returns:
            The pattern configuration or None
        """
        query = select(self.model).where(self.model.pattern_name == pattern_name).order_by(self.model.updated_at.desc())  # type: ignore

        result = await self.session.execute(query)
        return cast(PatternConfig | None, result.scalars().first())

    async def list_configs(self) -> list[PatternConfig]:
        """
        List all pattern configurations for a tenant.

        Returns:
            List of pattern configurations
        """
        query = select(self.model).order_by(self.model.updated_at.desc())  # type: ignore

        result = await self.session.execute(query)
        return cast(list[PatternConfig], result.scalars().all())

    async def create_or_update_config(self, config: PatternConfigModel | PatternConfig) -> PatternConfig:
        """
        Create or update a pattern configuration.

        Args:
            config: The pattern configuration

        Returns:
            The created or updated pattern configuration
        """
        # Convert from pydantic model if needed
        if isinstance(config, PatternConfig):
            config = config.to_pydantic()

        # Check if a configuration already exists
        existing = await self.get_config(config.pattern_name)

        if existing:
            # Update existing configuration
            config = await self.update(obj=existing, obj_in=config)
        else:
            # Create a new configuration
            config = await self.create(obj_in=config)
        return config

    async def delete_config(self, pattern_name: str) -> bool:
        """
        Delete a pattern configuration.

        Args:
            pattern_name: The name of the pattern

        Returns:
            True if the configuration was deleted
        """
        config = await self.get_config(pattern_name)
        if not config:
            return False

        await self.session.delete(config)
        await self.session.commit()
        return True
