"""
Dependencies for semantic manager module.

This module provides FastAPI dependency functions for the semantic manager module.
"""

from typing import Annotated

from fastapi import Depends

from query_manager.db.config import AsyncSessionDep
from query_manager.semantic_manager.crud import SemanticManager


async def get_semantic_manager(session: AsyncSessionDep) -> SemanticManager:
    return SemanticManager(session)


SemanticManagerDep = Annotated[SemanticManager, Depends(get_semantic_manager)]
